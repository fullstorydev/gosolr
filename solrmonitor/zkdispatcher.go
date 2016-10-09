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

package solrmonitor

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Handle a zk event.  Optionally return a new event channel to automatically re-register.
type ZkEventHandler func(zk.Event) <-chan zk.Event

// Monitors many zk.Event channels on a single goroutine.
type ZkDispatcher struct {
	logger         zk.Logger            // where to debug log
	selectChan     chan newHandler      // sends new handlers to the event loop, or signals exit
	selectCases    []reflect.SelectCase // the list of event channels to watch
	selectHandlers []ZkEventHandler     // the list of handlers associated with each event channel
	runningProcs   int32                // for testing

	mu       sync.Mutex
	isClosed bool
}

type newHandler struct {
	watcher <-chan zk.Event
	handler ZkEventHandler
}

func NewZkDispatcher(logger zk.Logger) *ZkDispatcher {
	selectChan := make(chan newHandler, 64)
	d := &ZkDispatcher{
		logger:     logger,
		selectChan: selectChan,
		selectCases: []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(selectChan)},
		},
		selectHandlers: []ZkEventHandler{
			nil, // No handler for case 0, it's the special exit handler.
		},
		runningProcs: 1,
	}
	go d.eventLoop()
	return d
}

func (d *ZkDispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isClosed {
		panic("already closed")
	}
	d.isClosed = true
	close(d.selectChan)
}

var errClosed = errors.New("already closed")

// Watch a new ZK event.
// Warning: do not call this function more than 64 times while synchronously dispatching an event.
func (d *ZkDispatcher) Watch(watcher <-chan zk.Event, handler ZkEventHandler) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isClosed {
		return errClosed
	}

	select {
	case d.selectChan <- newHandler{watcher, handler}:
		return nil
	case <-time.After(5 * time.Second):
		panic("channel is full")
	}
	return nil
}

func (d *ZkDispatcher) eventLoop() {
	defer atomic.AddInt32(&d.runningProcs, -1)

	for {
		chosen, recv, recvOK := reflect.Select(d.selectCases)
		if chosen == 0 {
			if recvOK {
				// New handler
				nh := recv.Interface().(newHandler)
				d.selectHandlers = append(d.selectHandlers, nh.handler)
				d.selectCases = append(d.selectCases, newCase(nh.watcher))
				continue
			} else {
				// closing.
				d.logger.Printf("zkdispatcher: exiting via close channel")
				break
			}
		} else {
			evt := recv.Interface().(zk.Event)
			if evt.Type == 0 || evt.Err == zk.ErrClosing {
				// ZK client closed, stop looping.
				d.logger.Printf("zkdispatcher: exiting because ZK is closing")
				break
			}

			newWatcher := d.selectHandlers[chosen](evt)
			if newWatcher != nil {
				d.selectCases[chosen] = newCase(newWatcher)
			} else {
				// ZK event channels are one-shot; remove the case and handler.
				d.selectHandlers = append(d.selectHandlers[:chosen], d.selectHandlers[chosen+1:]...)
				d.selectCases = append(d.selectCases[:chosen], d.selectCases[chosen+1:]...)
			}
		}
	}
}

func newCase(watcher <-chan zk.Event) reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(watcher)}
}
