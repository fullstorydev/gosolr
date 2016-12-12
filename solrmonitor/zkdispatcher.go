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

// ugh... seems silly to have to define this
const maxInt = int(^uint(0) >> 1)

// A function that handles a zk event. Optionally returns a new event channel to automatically
// re-register.
type ZkEventHandler func(zk.Event) <-chan zk.Event

// An interface that handles a zk event. Optionally returns a new event channel to automatically
// re-register.
type ZkEventCallback interface {
	Handle(zk.Event) <-chan zk.Event
}

// Adapts a ZkEventHandler function to the ZkEventCallback interface.
type zkEventHandlerAdapter struct {
	handler ZkEventHandler
}

func (a *zkEventHandlerAdapter) Handle(event zk.Event) <-chan zk.Event {
	return a.handler(event)
}

// Monitors many zk.Event channels. Dispatches handling to other goroutines allowing handlers to
// run in parallel. A single ZkEventCallback, however, will always be invoked as if from the
// same goroutine. So a single callback can be used to watch multiple channels and it will receive
// events in a way that does not require it to synchronize with other possible invocations.
type ZkDispatcher struct {
	logger         zk.Logger            // where to debug log
	selectCases    []reflect.SelectCase // the list of event channels to watch
	selectHandlers []ZkEventCallback    // the list of handlers associated with each event channel
	runningProcs   int32                // for testing

	chanMu     sync.Mutex
	isClosed   bool
	selectChan chan newHandler // sends new handlers to the event loop, or signals exit

	taskMu sync.Mutex
	tasks  map[ZkEventCallback]*fifoTaskQueue
}

type newHandler struct {
	watcher <-chan zk.Event
	handler ZkEventCallback
}

func NewZkDispatcher(logger zk.Logger) *ZkDispatcher {
	selectChan := make(chan newHandler, 64)
	d := &ZkDispatcher{
		logger:     logger,
		selectChan: selectChan,
		selectCases: []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(selectChan)},
		},
		selectHandlers: []ZkEventCallback{
			nil, // No handler for case 0, it's the special exit handler.
		},
		runningProcs: 1,
		tasks:        make(map[ZkEventCallback]*fifoTaskQueue),
	}
	go d.eventLoop()
	return d
}

func (d *ZkDispatcher) Close() {
	d.chanMu.Lock()
	defer d.chanMu.Unlock()
	if d.isClosed {
		panic("already closed")
	}
	d.isClosed = true
	close(d.selectChan)
}

var errClosed = errors.New("already closed")

// Watch a new ZK event using the given callback to handle the event.
func (d *ZkDispatcher) WatchEvent(watcher <-chan zk.Event, handler ZkEventCallback) error {
	d.chanMu.Lock()
	defer d.chanMu.Unlock()
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

// Watch a new ZK event using the given function to handle the event.
func (d *ZkDispatcher) Watch(watcher <-chan zk.Event, handler ZkEventHandler) error {
	return d.WatchEvent(watcher, &zkEventHandlerAdapter{handler})
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
			d.invokeTask(zkDispatchTask{d.selectHandlers[chosen], evt})
			// ZK event channels are one-shot; remove the case and handler.
			d.selectHandlers = append(d.selectHandlers[:chosen], d.selectHandlers[chosen+1:]...)
			d.selectCases = append(d.selectCases[:chosen], d.selectCases[chosen+1:]...)
		}
	}
}

func (d *ZkDispatcher) invokeTask(task zkDispatchTask) {
	d.taskMu.Lock()
	defer d.taskMu.Unlock()

	q := d.tasks[task.callback]
	if q != nil {
		q.add(task)
	} else {
		q = &fifoTaskQueue{}
		d.tasks[task.callback] = q
		q.add(task)
		// must start a worker for this callback
		go d.worker(task)
	}
}

func (d *ZkDispatcher) worker(initialTask zkDispatchTask) {
	cb := initialTask.callback
	task := initialTask
	for {
		newWorker := task.callback.Handle(task.event)
		if newWorker != nil {
			// this may return err if the dispatcher is concurrently closed,
			// but that's fine -- we just skip the watch if closed
			d.WatchEvent(newWorker, task.callback)
		}
		var ok bool
		if task, ok = d.dequeueTask(cb); !ok {
			return
		}
	}
}

func (d *ZkDispatcher) dequeueTask(cb ZkEventCallback) (zkDispatchTask, bool) {
	d.taskMu.Lock()
	defer d.taskMu.Unlock()
	// We leave the head in the queue while it's being processed. That way we can know when a
	// worker goroutine is allowed to exit because its queue becomes empty, and  at the same
	// time we're better able to re-use workers because their queue isn't empty while they are
	// processing the tail (so additional items can be enqueued that will cause the goroutine
	// to continue working).

	// So first we remove the old head.
	q := d.tasks[cb]
	q.poll()
	// Then return the next one.
	if q.size == 0 {
		// remove item from map to make sure map does
		// not grow unbounded
		delete(d.tasks, cb)
		return zkDispatchTask{}, false
	}
	return q.peek()
}

func newCase(watcher <-chan zk.Event) reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(watcher)}
}
