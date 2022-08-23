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

// a tool that just monitors and logs changes to a solr cluster
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/fullstorydev/zk"
)

var (
	zkServers = flag.String("zkServers", "127.0.0.1:2181/solr", `comma separated list of the zk servers solr is using; ip:port or hostname:port, followed by /solr`)
	flakyFlag = flag.Bool("flaky", false, "emulate flaky ZK connection to test core logic")
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

	installStackTraceDumper()

	zkHosts, solrZkPath, err := smutil.ParseZkServersFlag(*zkServers)
	if err != nil {
		return smutil.Cherrf(err, "error parsing -zkServers flag %s", *zkServers)
	}

	logger.Printf("Starting solrmonitor with zkHosts=%v solrZkPath=%s", zkHosts, solrZkPath)

	zkWatcher := solrmonitor.NewZkWatcherMan(logger)
	zooClient, err := NewZkConn(logger, zkHosts, zkWatcher.EventCallback)
	if err != nil {
		return smutil.Cherrf(err, "Failed to connect to zookeeper")
	}
	zkCli := solrmonitor.ZkCli(zooClient)

	var flakyZk *sexPantherZkCli
	if *flakyFlag {
		flakyZk = &sexPantherZkCli{
			delegate: zooClient,
			rnd:      rand.New(rand.NewSource(0)),
			flaky:    0, // during startup
		}
		zkCli = flakyZk
	}

	solrMonitor, err := solrmonitor.NewSolrMonitorWithRoot(zkCli, zkWatcher, &solrmonitorLogger{logger: logger}, solrZkPath, nil)
	if err != nil {
		return smutil.Cherrf(err, "Failed to create solrMonitor")
	}
	defer solrMonitor.Close()

	if *flakyFlag {
		// Now that we're up and running, make it flaky
		flakyZk.setFlaky(true)
	}

	logger.Println("Waiting for interrupt...")
	awaitSignal()

	logger.Println("exiting...")
	return nil
}

func installStackTraceDumper() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGQUIT)

	buf := make([]byte, 10*1024*1024) // reserve 10mb
	go func() {
		for range sigChan {
			stacklen := runtime.Stack(buf, true)
			fmt.Fprintf(os.Stderr, "=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()
}

func awaitSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
}

func NewZkConn(logger *log.Logger, servers []string, cb zk.EventCallback) (*zk.Conn, error) {
	conn, events, err := zk.Connect(servers, 10*time.Second, func(conn *zk.Conn) {
		conn.SetLogger(&zookeeperLogger{logger: logger})
	}, zk.WithEventCallback(cb))
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

	go func() {
		for evt := range events {
			if evt.Path != "" {
				if evt.Err != nil {
					logger.Printf("zk %s event received for %s with err %v", evt.Type, evt.Path, evt.Err)
				} else {
					logger.Printf("zk %s event received for %s", evt.Type, evt.Path)
				}
			} else {
				logger.Printf("zk %s event received with state %s", evt.Type, evt.State)
			}
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

type solrmonitorLogger struct {
	logger *log.Logger
}

var _ zk.Logger = &solrmonitorLogger{}

func (l *solrmonitorLogger) Printf(format string, args ...interface{}) {
	l.logger.Printf("solrmonitor: "+format, args...)
}

// TODO: make an integration test using this idea.

// 60% of the time, it works every time
const flakeChance = 0.60

type sexPantherZkCli struct {
	delegate solrmonitor.ZkCli
	rnd      *rand.Rand
	flaky    int32
}

var _ solrmonitor.ZkCli = &sexPantherZkCli{}

func (s *sexPantherZkCli) setFlaky(flaky bool) {
	if flaky {
		atomic.StoreInt32(&s.flaky, 1)
	} else {
		atomic.StoreInt32(&s.flaky, 0)
	}
}

func (s *sexPantherZkCli) isFlaky() bool {
	return atomic.LoadInt32(&s.flaky) != 0
}

func (s *sexPantherZkCli) Children(path string) ([]string, *zk.Stat, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return nil, nil, errors.New("flaky error")
	}
	return s.delegate.Children(path)
}

func (s *sexPantherZkCli) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return "", errors.New("flaky error")
	}
	return s.delegate.Create(path, data, flags, acl)
}

func (s *sexPantherZkCli) Delete(path string, version int32) error {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return errors.New("flaky error")
	}
	return s.delegate.Delete(path, version)
}

func (s *sexPantherZkCli) Exists(path string) (bool, *zk.Stat, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return false, nil, errors.New("flaky error")
	}
	return s.delegate.Exists(path)
}

func (s *sexPantherZkCli) SessionID() int64 {
	return s.delegate.SessionID()
}

func (s *sexPantherZkCli) Set(path string, contents []byte, version int32) (*zk.Stat, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return nil, errors.New("flaky error")
	}
	return s.delegate.Set(path, contents, version)
}

func (s *sexPantherZkCli) Sync(path string) (string, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return "", errors.New("flaky error")
	}
	return s.delegate.Sync(path)
}

func (s *sexPantherZkCli) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return nil, nil, nil, errors.New("flaky error")
	}
	return s.delegate.ChildrenW(path)
}

func (s *sexPantherZkCli) Get(path string) ([]byte, *zk.Stat, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return nil, nil, errors.New("flaky error")
	}
	return s.delegate.Get(path)
}

func (s *sexPantherZkCli) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return nil, nil, nil, errors.New("flaky error")
	}
	return s.delegate.GetW(path)
}

func (s *sexPantherZkCli) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.rnd.Float32() > flakeChance {
		return false, nil, nil, errors.New("flaky error")
	}
	return s.delegate.ExistsW(path)
}

func (s *sexPantherZkCli) State() zk.State {
	return s.delegate.State()
}

func (s *sexPantherZkCli) Close() {
	s.delegate.Close()
}
