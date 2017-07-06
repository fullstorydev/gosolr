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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkServers = flag.String("zkServers", "127.0.0.1:2181/solr", `comma separated list of the zk servers solr is using; ip:port or hostname:port, followed by /solr`)
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

	zkHosts, solrZkPath, err := smutil.ParseZkServersFlag(*zkServers)
	if err != nil {
		return smutil.Cherrf(err, "error parsing -zkServers flag %s", *zkServers)
	}

	logger.Printf("Starting solrmonitor with zkHosts=%v solrZkPath=%s", zkHosts, solrZkPath)

	zooClient, err := NewZkConn(logger, zkHosts)
	if err != nil {
		return smutil.Cherrf(err, "Failed to connect to zookeeper")
	}
	defer zooClient.Close()

	solrMonitor, err := solrmonitor.NewSolrMonitorWithRoot(zooClient, &solrmonitorLogger{logger: logger}, solrZkPath)
	if err != nil {
		return smutil.Cherrf(err, "Failed to create solrMonitor")
	}
	defer solrMonitor.Close()

	logger.Println("Waiting for interrupt...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)

	logger.Println("exiting...")
	return nil
}

func NewZkConn(logger *log.Logger, servers []string) (*zk.Conn, error) {
	conn, events, err := zk.Connect(servers, 10*time.Second, func(conn *zk.Conn) {
		conn.SetLogger(&zookeeperLogger{logger: logger})
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

type solrmonitorLogger struct {
	logger *log.Logger
}

var _ zk.Logger = &solrmonitorLogger{}

func (l *solrmonitorLogger) Printf(format string, args ...interface{}) {
	l.logger.Printf("solrmonitor: "+format, args...)
}
