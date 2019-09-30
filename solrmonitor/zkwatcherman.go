// Copyright 2017 FullStory, Inc.
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

package solrmonitor

import (
	"fmt"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// A monitorChildren request whose last attempt to set a watch failed.
type deferredChildrenTask struct {
	path     string
	onchange func(children []string) (bool, error)
}

// A monitorData request whose last attempt to set a watch failed.
type deferredDataTask struct {
	path     string
	onchange func(data string, version int32) bool
}

// Helper class to continuously monitor nodes for state or data changes.
type zkWatcherMan struct {
	mu         sync.RWMutex
	logger     zk.Logger // where to debug log
	zkCli      ZkCli     // the ZK client
	dispatcher *ZkDispatcher

	deferredTasksNotEmpty chan struct{} // signals the monitor loop that a new task was enqueued
	deferredTaskMu        sync.Mutex    // guards deferredRecoveryTasks
	deferredRecoveryTasks fifoTaskQueue
}

// Create a zkWatcherMan to continuously monitor nodes for state and data changes.
// In the event of a zk disconnect and reconnect, will automatically re-establish watches.
func NewZkWatcherMan(logger zk.Logger, zkCli ZkCli) *zkWatcherMan {
	ret := &zkWatcherMan{
		logger:                logger,
		zkCli:                 zkCli,
		dispatcher:            NewZkDispatcher(logger),
		deferredTasksNotEmpty: make(chan struct{}, 1),
	}
	ret.Start()
	return ret
}

func (m *zkWatcherMan) Start() {
	go func() {
		backoffDuration := 5 * time.Millisecond
		sleepBackoff := func() {
			select {
			case <-time.After(backoffDuration):
				backoffDuration *= 2
				if backoffDuration > time.Second {
					backoffDuration = time.Second
				}
			case <-m.dispatcher.closedChan:
			}
		}
		resetBackoff := func() {
			backoffDuration = 5 * time.Millisecond
		}

		for {
			select {
			case <-m.dispatcher.closedChan:
				return
			case <-m.deferredTasksNotEmpty:
				for {
					polled := func() interface{} {
						m.deferredTaskMu.Lock()
						defer m.deferredTaskMu.Unlock()
						if polled, ok := m.deferredRecoveryTasks.poll(); ok {
							return polled
						} else {
							return nil
						}
					}()

					if polled == nil {
						break
					}

					success := true
					switch task := polled.(type) {
					case deferredChildrenTask:
						err := m.monitorChildren(true, task.path, task.onchange)
						if err != nil {
							success = false
							m.logger.Printf("ERROR: monitorChildren %s failed: %s", task.path, err)
						}
					case deferredDataTask:
						err := m.monitorData(true, task.path, task.onchange)
						if err != nil {
							success = false
							m.logger.Printf("ERROR: monitorData %s failed: %s", task.path, err)
						}
					default:
						panic(fmt.Sprintf("Unexpected item in taskqueue %+v", task))
					}

					if success {
						// Keep going
						resetBackoff()
						continue
					} else {
						// re-enqueue the task, sleep, then loop around and try again later
						m.enqueueDeferredTask(polled)
						sleepBackoff()
						break
					}
				}
			}
		}
	}()
}

func (m *zkWatcherMan) Close() {
	m.dispatcher.Close()
}

func (m *zkWatcherMan) enqueueDeferredTask(task interface{}) {
	m.deferredTaskMu.Lock()
	defer m.deferredTaskMu.Unlock()
	m.deferredRecoveryTasks.add(task)

	// Notify the processing loop that the queue is not empty, but don't block.
	select {
	case m.deferredTasksNotEmpty <- struct{}{}:
	default:
	}
}

func (m *zkWatcherMan) monitorChildren(synch bool, path string, onchange func(children []string) (bool, error)) error {
	if !synch {
		// Just setup a deferred task and call it a day
		m.enqueueDeferredTask(deferredChildrenTask{path: path, onchange: onchange})
		return nil // fake success
	}

	// Initial synchronous call
	children, watcher, err := getChildrenAndWatch(m.zkCli, path)
	if err != nil {
		return err
	}
	cont, err := onchange(children)
	if err != nil {
		return err
	}
	if !cont {
		return nil
	}

	// Setup monitoring
	f := func(evt zk.Event) <-chan zk.Event {
		m.logger.Printf("zkWatcherMan children %s: %s", path, evt)
		children, newWatcher, err := getChildrenAndWatch(m.zkCli, path)
		if err != nil {
			if err != zk.ErrClosing {
				// We failed to set a watch; add a task for the recovery thread to keep trying
				m.logger.Printf("zkWatcherMan %s: error getting children: %s", path, err)
				m.enqueueDeferredTask(deferredChildrenTask{path: path, onchange: onchange})
			}
			return nil
		}
		cont, err := onchange(children)
		if err != nil {
			// We failed to call the onchange handler; add a task for the recovery thread to keep trying
			m.logger.Printf("zkWatcherMan %s: error calling onchange: %s", path, err)
			m.enqueueDeferredTask(deferredChildrenTask{path: path, onchange: onchange})
			return nil
		} else if cont {
			return newWatcher
		} else {
			return nil // don't watch anymore
		}
	}
	m.dispatcher.Watch(watcher, f)
	return nil
}

func (m *zkWatcherMan) monitorData(synch bool, path string, onchange func(data string, version int32) bool) error {
	if !synch {
		// Just setup a deferred task and call it a day
		m.enqueueDeferredTask(deferredDataTask{path: path, onchange: onchange})
		return nil // fake success
	}

	// Initial synchronous call
	data, version, watcher, err := getDataAndWatch(m.zkCli, path)
	if err != nil {
		return err
	}
	cont := onchange(data, version)
	if !cont {
		return nil
	}

	// Setup monitoring
	f := func(evt zk.Event) <-chan zk.Event {
		logger := m.logger
		zkCli := m.zkCli
		logger.Printf("zkWatcherMan data %s: %s", path, evt)
		data, version, newWatcher, err := getDataAndWatch(zkCli, path)
		if err != nil {
			if err != zk.ErrClosing {
				// We failed to set a watch; add a task for the recovery thread to keep trying
				logger.Printf("zkWatcherMan %s: error getting data: %s", path, err)
				m.enqueueDeferredTask(deferredDataTask{path: path, onchange: onchange})
			}
			return nil
		}

		cont := onchange(data, version)
		if cont {
			return newWatcher
		} else {
			return nil // don't watch anymore
		}
	}
	m.dispatcher.Watch(watcher, f)
	return nil
}

func getChildrenAndWatch(zkCli ZkCli, path string) ([]string, <-chan zk.Event, error) {
	for {
		children, _, childrenWatch, err := zkCli.ChildrenW(path)
		if err == nil {
			// Success, we're done.
			return children, childrenWatch, nil
		}

		if err == zk.ErrNoNode {
			// Node doesn't exist; add an existence watch.
			exists, _, existsWatch, err := zkCli.ExistsW(path)
			if err != nil {
				return nil, nil, err
			}
			if exists {
				// Improbable, but possible; first we checked and it wasn't there, then we checked and it was.
				// Just loop and try again.
				continue
			}
			// Node still doesn't exist, return empty list and exists watch
			return nil, existsWatch, err
		}

		return nil, nil, err
	}
}

func getDataAndWatch(zkCli ZkCli, path string) (string, int32, <-chan zk.Event, error) {
	for {
		data, stat, dataWatch, err := zkCli.GetW(path)
		if err == nil {
			// Success, we're done.
			return string(data), stat.Version, dataWatch, nil
		}

		if err == zk.ErrNoNode {
			// Node doesn't exist; add an existence watch.
			exists, _, existsWatch, err := zkCli.ExistsW(path)
			if err != nil {
				return "", -1, nil, err
			}
			if exists {
				// Improbable, but possible; first we checked and it wasn't there, then we checked and it was.
				// Just loop and try again.
				continue
			}
			return "", -1, existsWatch, nil
		}

		return "", -1, nil, err
	}
}
