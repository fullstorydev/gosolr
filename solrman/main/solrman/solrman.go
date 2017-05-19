// Copyright 2016 FullStory, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// main solrman application; web server and task manager; automatically manages a solr cluster
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"encoding/json"

	"github.com/fullstorydev/gosolr/solrman/smservice"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/garyburd/redigo/redis"
	"github.com/hjr265/redsync.go/redsync"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	MaxIdleConnsPerHost  = 1024
	SolrmanMutexRedisKey = "Solrman:mutex"
)

var (
	port        = flag.Int("port", 8984, "http port to listen on for local admin / queries")
	zkServers   = flag.String("zkServers", "127.0.0.1:2181", "comma separated list of the zk servers solr is using; ip:port or hostname:port")
	redisServer = flag.String("redisServer", "127.0.0.1:6379", "redis server to store solrman's internal state")
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	if err := run(logger); err != nil {
		logger.Fatalf("ERROR: %s", err.Error())
	}
	logger.Println("success")
}

func run(logger *log.Logger) error {
	flag.Parse()

	if len(flag.Args()) > 0 {
		panic("This program does not take arguments.")
	}

	logger.Printf("Solrman is starting up: http port=%d; zkServers=%s; redisServer=%s", *port, *zkServers, *redisServer)

	zkLogger := &zookeeperLogger{logger: logger}
	smLogger := &solrmanLogger{logger: logger}
	smAudit := &solrmanAudit{logger: logger}

	// Solrman makes a lot of requests to Solr; without increasing MaxIdleConnsPerHost we can run out of available sockets,
	// which manifests as errors of the form "cannot assign requested address"
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = MaxIdleConnsPerHost

	zooClient, err := NewZkConn(zkLogger, strings.Split(*zkServers, ","))
	if err != nil {
		return err
	}
	defer zooClient.Close()

	solrMonitor, err := solrmonitor.NewSolrMonitorWithLogger(zooClient, zkLogger)
	if err != nil {
		return err
	}
	defer solrMonitor.Close()

	httpClient := &http.Client{}

	redisPool := NewRedisPool(*redisServer)
	if err := func() error {
		conn := redisPool.Get()
		defer conn.Close()
		if _, err := conn.Do("PING"); err != nil {
			return fmt.Errorf("Failed to connect redis on first try: %s", err)
		}
		return nil
	}(); err != nil {
		return err
	}

	mutex, err := acquireAndMonitorRedisMutex(smLogger, redisPool)
	if err != nil {
		return fmt.Errorf("Failed to acquire Solrman redis mutex: %s", err)
	}
	smLogger.Infof("Acquired Solrman redis mutex")
	defer mutex.Unlock()

	storage, err := smservice.NewZkStorage(zooClient, "/solrman", smLogger)
	if err != nil {
		return fmt.Errorf("Failed to open zk storage: %s", err)
	}

	solrManService := &smservice.SolrManService{
		HttpClient:  httpClient,
		SolrMonitor: solrMonitor,
		ZooClient:   zooClient,
		Storage:     storage,
		Logger:      smLogger,
		AlertLog:    smLogger,
		Audit:       smAudit,
	}

	solrManService.Init()
	// Run the automatic solr management loop.
	go solrManService.RunSolrMan()

	http.HandleFunc("/clusterState", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}

		rsp, err := solrManService.ClusterState()
		if err != nil {
			smLogger.Errorf("error handling request %v: %s", req, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		if err := json.NewEncoder(w).Encode(rsp); err != nil {
			smLogger.Errorf("failed to json-encode response: %s", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	http.HandleFunc("/moveShard", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}

		query := req.URL.Query()
		moveReq := &solrmanapi.MoveShardRequest{
			Collection: query.Get("collection"),
			Shard:      query.Get("shard"),
			SrcNode:    query.Get("srcNode"),
			DstNode:    query.Get("dstNode"),
		}

		rsp, err := solrManService.MoveShard(moveReq)
		if err != nil {
			smLogger.Errorf("error handling request %v: %s", req, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if rsp.Error != "" {
			w.WriteHeader(http.StatusBadRequest)
		}

		if err := json.NewEncoder(w).Encode(rsp); err != nil {
			smLogger.Errorf("failed to json-encode response: %s", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	http.HandleFunc("/splitShard", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}

		query := req.URL.Query()
		moveReq := &solrmanapi.SplitShardRequest{
			Collection: query.Get("collection"),
			Shard:      query.Get("shard"),
		}

		rsp, err := solrManService.SplitShard(moveReq)
		if err != nil {
			smLogger.Errorf("error handling request %v: %s", req, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if rsp.Error != "" {
			w.WriteHeader(http.StatusBadRequest)
		}

		if err := json.NewEncoder(w).Encode(rsp); err != nil {
			smLogger.Errorf("failed to json-encode response: %s", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	// start http server
	smLogger.Infof("Listening on port %d", *port)
	httpServer := &http.Server{
		Addr:         fmt.Sprintf("127.0.0.1:%d", *port),
		Handler:      http.DefaultServeMux,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	if err := ListenAndServe(httpServer); err != nil {
		return fmt.Errorf("Failed to start http server: %s", err)
	}

	smLogger.Debugf("exiting...")
	return nil
}

// Like http.ListenAndServe() but handles closing gracefully
func ListenAndServe(srv *http.Server) error {
	addr := srv.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	var didSignal int32
	go func() {
		signals := make(chan os.Signal, 10)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-signals:
			atomic.StoreInt32(&didSignal, 1)
			signal.Stop(signals)
			ln.Close()
			return
		}
	}()

	err = srv.Serve(ln)
	if atomic.LoadInt32(&didSignal) == 0 {
		return err
	}
	return nil // just ignore any error if we closed the server ourselves
}

// Create a redis mutex to ensure we're the only solrman; 2 solrman instances would be bad.
func acquireAndMonitorRedisMutex(logger *solrmanLogger, redisPool *redis.Pool) (*redsync.Mutex, error) {
	logger.Infof("Acquiring redis mutex...")
	mutex, err := redsync.NewMutexWithPool(SolrmanMutexRedisKey, []*redis.Pool{redisPool})
	if err != nil {
		return nil, fmt.Errorf("Failed to create redis mutex: %s", err)
	}
	// Lock lasts 1 minute, we refresh it every 30 seconds in the steady state.
	// For initial acquisition, try every 5 seconds for just over the expiry time.
	// If we can't acquire the lock in 65 seconds we're probably not going to because someone else has it.
	mutex.Expiry = 60 * time.Second
	mutex.Delay = 5 * time.Second
	mutex.Tries = 12 + 1
	if err := mutex.Lock(); err != nil {
		return nil, fmt.Errorf("Failed to acquire redis mutex: %s", err)
	}

	// Monitor the mutex and touch it every 30 seconds; if we lose the mutex (somehow) shut down.
	go func() {
		for {
			time.Sleep(30 * time.Second)
			touched := mutex.Touch()
			if !touched {
				// try to re-acquire the lock
				logger.Warningf("Lost Solrman mutex, attempting to re-acquire")
				mutex.Unlock()
				if mutex.Lock() == nil {
					logger.Warningf("Re-acquired Solrman mutex")
					continue
				}
				logger.Errorf("Lost Solrman redis mutex, exiting")
				os.Exit(1)
			}
		}
	}()

	return mutex, nil
}

type solrmanLogger struct {
	logger *log.Logger
}

var _ smservice.Logger = &solrmanLogger{}

func (l *solrmanLogger) Debugf(format string, args ...interface{}) {
	l.logger.Printf("DEBUG: "+format, args...)
}

func (l *solrmanLogger) Infof(format string, args ...interface{}) {
	l.logger.Printf("INFO: "+format, args...)
}

func (l *solrmanLogger) Warningf(format string, args ...interface{}) {
	l.logger.Printf("WARN: "+format, args...)
}

func (l *solrmanLogger) Errorf(format string, args ...interface{}) {
	l.logger.Printf("ERROR: "+format, args...)
}

type solrmanAudit struct {
	logger *log.Logger
}

var _ smservice.Audit = &solrmanAudit{}

func (a *solrmanAudit) BeforeOp(op solrmanapi.OpRecord, collState solrmonitor.CollectionState) {
	a.recordOp("BeforeOp", &op, &collState)
}

func (a *solrmanAudit) SuccessOp(op solrmanapi.OpRecord, collState solrmonitor.CollectionState) {
	a.recordOp("SuccessOp", &op, &collState)
}

func (a *solrmanAudit) FailedOp(op solrmanapi.OpRecord, collState solrmonitor.CollectionState) {
	a.recordOp("FailedOp", &op, &collState)
}

func (a *solrmanAudit) recordOp(opState string, op *solrmanapi.OpRecord, collState *solrmonitor.CollectionState) {
	if json, err := json.MarshalIndent(collState, "", "  "); err != nil {
		a.logger.Printf("Error marshaling collState for %s %s, Version %d: %s", opState, op, collState.ZkNodeVersion, err)
	} else {
		a.logger.Printf("%s: %s, Version %d:\n%s", opState, op, collState.ZkNodeVersion, string(json))
	}
}

func NewZkConn(logger zk.Logger, servers []string) (*zk.Conn, error) {
	conn, events, err := zk.Connect(servers, 10*time.Second, func(conn *zk.Conn) {
		conn.SetLogger(logger)
	})
	if err != nil {
		return nil, err
	}

	c := time.After(10 * time.Second)
loop:
	for {
		select {
		case <-c:
			conn.Close()
			return nil, errors.New("zk connect deadline exceeded")
		case evt := <-events:
			logger.Printf("zk: startup state change: %v", evt.State)
			if evt.State == zk.StateHasSession {
				break loop
			}
		}
	}

	// start a goroutine to log state-change events
	go func() {
		for evt := range events {
			logger.Printf("zk event: %v", evt)
		}
	}()

	return conn, nil
}

type zookeeperLogger struct {
	logger *log.Logger
}

var _ zk.Logger = &zookeeperLogger{}

func (l *zookeeperLogger) Printf(format string, args ...interface{}) {
	l.logger.Printf("zk: "+format, args...)
}

func NewRedisPool(redisServer string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		MaxActive:   100,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			// made up timeouts to avoid blocking too long on redis
			return redis.Dial("tcp", redisServer,
				redis.DialConnectTimeout(50*time.Millisecond),
				redis.DialReadTimeout(50*time.Millisecond),
			)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// made up heuristic: test the connection if it's been idle > minute.
			if time.Now().Sub(t) > time.Minute {
				_, err := c.Do("PING")
				return err
			}
			return nil
		},
	}
}
