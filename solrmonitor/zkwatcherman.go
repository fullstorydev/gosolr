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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fullstorydev/zk"
)

type Callbacks interface {
	ChildrenChanged(path string, children []string) error
	DataChanged(path string, data string, stat *zk.Stat) error
	ShouldWatchChildren(path string) bool
	ShouldWatchData(path string) bool
}

// A MonitorChildren request whose last attempt to set a watch failed.
type deferredChildrenTask struct {
	path string
}

// A MonitorData request whose last attempt to set a watch failed.
type deferredDataTask struct {
	path string
}

// Helper class to continuously monitor nodes for state or data changes.
type ZkWatcherMan struct {
	ctx    context.Context // when this closes, exit
	cancel func()          // function to cancel / close

	logger    zk.Logger // where to debug log
	zkCli     ZkCli     // the ZK client
	callbacks Callbacks

	deferredTasksNotEmpty chan struct{} // signals the monitor loop that a new task was enqueued
	deferredTaskMu        sync.Mutex    // guards deferredRecoveryTasks
	deferredRecoveryTasks fifoTaskQueue
}

// Create a ZkWatcherMan to continuously monitor nodes for state and data changes.
//
// The general usage is to wire up the returned instance's `EventCallback` method as the global event
// handler for a dedication zookeeper connection.
func NewZkWatcherMan(logger zk.Logger) *ZkWatcherMan {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &ZkWatcherMan{
		ctx:                   ctx,
		cancel:                cancel,
		logger:                logger,
		deferredTasksNotEmpty: make(chan struct{}, 1),
	}
	return ret
}

var _ zk.EventCallback = (*ZkWatcherMan)(nil).EventCallback

func (m *ZkWatcherMan) EventCallback(evt zk.Event) {
	switch evt.Type {
	case zk.EventNodeCreated, zk.EventNodeDeleted:
		// Just enqueue both kinds of tasks, we might throw one away later.
		m.logger.Printf("ZkWatcherMan %s: %s", evt.Type, evt.Path)
		m.enqueueDeferredTask(deferredDataTask{evt.Path})
		m.enqueueDeferredTask(deferredChildrenTask{evt.Path})
	case zk.EventNodeDataChanged:
		m.logger.Printf("ZkWatcherMan data %s: %s", evt.Type, evt.Path)
		m.enqueueDeferredTask(deferredDataTask{evt.Path})
	case zk.EventNodeChildrenChanged:
		m.logger.Printf("ZkWatcherMan children %s: %s", evt.Type, evt.Path)
		m.enqueueDeferredTask(deferredChildrenTask{evt.Path})
	case zk.EventNotWatching:
		// Lost ZK session; we'll need to re-register all watches when it comes back.
		// Just enqueue both kinds of tasks, we might throw them away later.
		m.enqueueDeferredTask(deferredDataTask{evt.Path})
		m.enqueueDeferredTask(deferredChildrenTask{evt.Path})
	default:
		if evt.Err == zk.ErrClosing {
			m.logger.Printf("ZkWatcherMan %s event received with state %s: closing", evt.Type, evt.State)
		} else if evt.Err != nil {
			m.logger.Printf("ZkWatcherMan %s event received with state %s with err: %v", evt.Type, evt.State, evt.Err)
		} else {
			m.logger.Printf("ZkWatcherMan %s event received with state %s", evt.Type, evt.State)
		}
	}
}

func (m *ZkWatcherMan) Start(zkCli ZkCli, callbacks Callbacks) {
	m.zkCli = zkCli
	m.callbacks = callbacks

	// Deferred task loop.
	go func() {
		backoffDuration := 5 * time.Millisecond
		sleepBackoff := func() {
			select {
			case <-time.After(backoffDuration):
				backoffDuration *= 2
				if backoffDuration > time.Second {
					backoffDuration = time.Second
				}
			case <-m.ctx.Done():
			}
		}
		resetBackoff := func() {
			backoffDuration = 5 * time.Millisecond
		}

		for {
			select {
			case <-m.ctx.Done():
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
						if callbacks.ShouldWatchChildren(task.path) {
							zkErr, cbErr := m.fetchChildren(task.path)
							if zkErr != nil {
								m.logger.Printf("zkwatcherman: error fetching children for %s: %s", task.path, zkErr)
							} else if cbErr != nil {
								m.logger.Printf("zkwatcherman: error in child callback for %s: %s", task.path, zkErr)
							}
							success = zkErr == nil
						}
					case deferredDataTask:
						if callbacks.ShouldWatchData(task.path) {
							if err := m.fetchAndNotifyCallback(task.path); err != nil {
								m.logger.Printf("zkwatcherman: error fetching data for %s: %s", task.path, err)
								success = false
							} else {
								success = true
							}
						}
					default:
						panic(fmt.Sprintf("Unexpected item in taskqueue %+v", task))
					}

					if success {
						// Keep going
						resetBackoff()
						continue
					} else {
						// Sleep, then loop around and try again later
						sleepBackoff()
						break
					}
				}
			}
		}
	}()
}

func (m *ZkWatcherMan) Close() {
	m.cancel()
}

func (m *ZkWatcherMan) enqueueDeferredTask(task interface{}) {
	m.deferredTaskMu.Lock()
	defer m.deferredTaskMu.Unlock()
	m.deferredRecoveryTasks.add(task)

	// Notify the processing loop that the queue is not empty, but don't block.
	select {
	case m.deferredTasksNotEmpty <- struct{}{}:
	default:
	}
}

// Begin monitoring the children of the given path, will resolve the current children before returning.
//
// Will return either a ZK error if the fetch failed, or propagate any errors returned from
// synchronously callbacks.
//
// Even if this method returns an error, ZkWacherMan will continuously attempt to monitor the given path.
func (m *ZkWatcherMan) MonitorChildren(path string) error {
	zkErr, cbErr := m.fetchChildren(path)
	if zkErr != nil {
		return zkErr
	}
	return cbErr
}

func (m *ZkWatcherMan) fetchChildren(path string) (zkErr, cbErr error) {
	if children, _, err := getChildrenAndWatch(m.zkCli, path); err != nil {
		if err == zk.ErrClosing {
			return nil, nil
		}
		// We failed to set a watch; add a task for the recovery thread to keep trying
		m.logger.Printf("ZkWatcherMan %s: error getting children: %s", path, err)
		m.enqueueDeferredTask(deferredChildrenTask{path: path})
		return err, nil
	} else {
		return nil, m.callbacks.ChildrenChanged(path, children)
	}
}

// Begin monitoring the data at the given path, will resolve the current data before returning.
//
// Will return either a ZK error if the fetch failed, or propagate any errors returned from
// synchronously callbacks.
//
// Even if this method returns an error, ZkWacherMan will continuously attempt to monitor the given path.
func (m *ZkWatcherMan) MonitorData(path string, recursive bool) error {
	//TODO handle reconnection???
	if _, err := m.addPersistentWatch(path, recursive); err != nil {
		return err
	}
	return m.fetchAndNotifyCallback(path)
}

// fetchAndNotifyCallback fetches the data of the path and then notifies the
// callbacks registered. No watch will be installed during this process.
//
// Take note that if such path does not exist, it will still notify the callbacks
// with empty "". This behavior is consistent with getDataAndWatch
func (m *ZkWatcherMan) fetchAndNotifyCallback(path string) error {
	dataBytes, stat, err := m.zkCli.Get(path)
	if err == zk.ErrClosing { //if closing simply return nil and do nothing
		return nil
	}

	var data string
	//for ErrNoNode, we use data = "". This is to maintain same behavior as previous getDataAndWatch
	if err == zk.ErrNoNode {
		data = ""
	} else if err != nil { //unexpected error
		return err
	} else {
		data = string(dataBytes)
	}

	m.callbacks.DataChanged(path, data, stat)
	return nil

}

func (m *ZkWatcherMan) StopMonitorData(path string) {
	m.zkCli.RemovePersistentWatch(path, nil)
}

// TODO flag on permanent or not
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

func (m *ZkWatcherMan) addPersistentWatch(path string, recursive bool) (<-chan zk.Event, error) {
	var addWatchMode zk.AddWatchMode
	if recursive {
		addWatchMode = zk.AddWatchModePersistentRecursive
		m.logger.Printf("Adding persistent watch (recursive) on %s", path)
	} else {
		addWatchMode = zk.AddWatchModePersistent
		m.logger.Printf("Adding persistent watch (non-recursive) on %s", path)
	}
	eventQueue, err := m.zkCli.AddPersistentWatch(path, addWatchMode)
	if err != nil {
		return nil, err
	}
	return wrapEventQueue(context.Background(), eventQueue), err
}

// wrapEventQueue wraps the EventQueue returned from the newer zk library back to the <-chan zk.Event used by
// existing caller.
func wrapEventQueue(ctx context.Context, q zk.EventQueue) <-chan zk.Event {
	eventCh := make(chan zk.Event)

	go func() {
		for {
			event, err := q.Next(ctx)
			if err != nil {
				return
			} else {
				eventCh <- event
			}
		}
	}()

	return eventCh
}
