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

// A function that handles a zk event. Optionally returns a new event channel to automatically
// re-register.
type ZkEventHandler func(zk.Event) <-chan zk.Event

// An interface that handles a zk event. Optionally returns a new event channel to automatically
// re-register.
type ZkEventCallback interface {
	Handle(zk.Event) <-chan zk.Event
}

func (h *ZkEventHandler) Handle(event zk.Event) <-chan zk.Event {
	return (*h)(event)
}

// zkDispatchTask is a queued task
type zkDispatchTask struct {
	callback ZkEventCallback
	event    zk.Event
}

// Monitors many zk.Event channels. Dispatches handling to other goroutines allowing handlers to
// run in parallel. A single ZkEventCallback, however, will always be invoked as if from the
// same goroutine. So a single callback can be used to watch multiple channels and it will receive
// events in a way that does not require it to synchronize with other possible invocations.
//
// Note: this class uses reflective select and is not efficient for very large numbers of watches (>1000).
type ZkDispatcher struct {
	logger         zk.Logger            // where to debug log
	selectCases    []reflect.SelectCase // the list of event channels to watch
	selectHandlers []ZkEventCallback    // the list of handlers associated with each event channel
	runningProcs   int32                // for testing
	closedChan     chan struct{}        // signals exit
	newHandlerChan chan newHandler      // sends new handlers to the event loop

	taskMu sync.Mutex
	tasks  map[ZkEventCallback]*fifoTaskQueue

	startedMu sync.Mutex
	started   bool
	// Appended to directly while holding the startedMu lock.
	// Once started == true, these are transferred over to the main working set and should not longer be modified.
	startCases    []reflect.SelectCase
	startHandlers []ZkEventCallback
}

type newHandler struct {
	watcher <-chan zk.Event
	handler ZkEventCallback
}

// NewZkDispatcher initializes a new instance of ZkDispatcher.
// Watches may be added to the returned instance at any point.
// Start must be called before watch handlers will be invoked.
// It is most efficient to set up all initial watches before making the call to Start.
//
// Note: this class uses reflective select and is not efficient for very large numbers of watches (>1000).
// Deprecated: use NewZkWatcherMan instead
func NewZkDispatcher(logger zk.Logger) *ZkDispatcher {
	closedChan := make(chan struct{})
	newHandlerChan := make(chan newHandler, 1024)
	d := &ZkDispatcher{
		logger:         logger,
		closedChan:     closedChan,
		newHandlerChan: newHandlerChan,
		selectCases: []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(closedChan)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(newHandlerChan)},
		},
		selectHandlers: []ZkEventCallback{nil, nil}, // first two correspond to close and new handler channels
		tasks:          make(map[ZkEventCallback]*fifoTaskQueue),
	}
	return d
}

// Start runs the event loop initialized with any watches registered up to this point.
// Watch handlers will only be invoked once the dispatcher has been started.
// Start may only be called once.
func (d *ZkDispatcher) Start() {
	d.startedMu.Lock()
	defer d.startedMu.Unlock()
	if d.started {
		panic("zk dispatcher already started")
	}
	d.started = true
	go d.eventLoop(d.startCases, d.startHandlers)
	d.startCases = nil
	d.startHandlers = nil
}

// Close stops the event loop.
// Watch handlers will no longer be invoked.
func (d *ZkDispatcher) Close() {
	close(d.closedChan)
}

var errClosed = errors.New("already closed")

// Watch a new ZK event using the given callback to handle the event.
func (d *ZkDispatcher) WatchEvent(watcher <-chan zk.Event, handler ZkEventCallback) error {
	// Check for closure first
	select {
	case <-d.closedChan:
		return errClosed
	default:
		break
	}

	// Optimization if the dipatcher has not been started yet, we should add the case and handler straight to
	// the slices to avoid unnecessary channel writing and event loop runs.
	added := func() bool {
		d.startedMu.Lock()
		defer d.startedMu.Unlock()
		if !d.started {
			d.startHandlers = append(d.startHandlers, handler)
			d.startCases = append(d.startCases, newCase(watcher))
			return true
		}
		return false
	}()
	if added {
		return nil
	}

	select {
	case <-d.closedChan:
		return errClosed
	case d.newHandlerChan <- newHandler{watcher, handler}:
		return nil
	case <-time.After(10 * time.Second):
		// Escape hatch to avoid deadlock
		panic("channel is full")
	}
}

// Watch a new ZK event using the given function to handle the event.
func (d *ZkDispatcher) Watch(watcher <-chan zk.Event, handler ZkEventHandler) error {
	return d.WatchEvent(watcher, &handler)
}

func (d *ZkDispatcher) eventLoop(startCases []reflect.SelectCase, startHandlers []ZkEventCallback) {
	atomic.AddInt32(&d.runningProcs, 1)
	defer atomic.AddInt32(&d.runningProcs, -1)

	d.selectHandlers = append(d.selectHandlers, startHandlers...)
	d.selectCases = append(d.selectCases, startCases...)

	for {
		// first try to drain any new handlers, bailing if dispatcher is closed
		for done := false; !done; {
			select {
			case nh := <-d.newHandlerChan:
				d.selectHandlers = append(d.selectHandlers, nh.handler)
				d.selectCases = append(d.selectCases, newCase(nh.watcher))
			case <-d.closedChan:
				d.logger.Printf("zkdispatcher: exiting via close channel")
				return
			default:
				done = true // nothing else waiting
			}
		}

		// then try to select an event from one of the watched channels
		chosen, recv, _ := reflect.Select(d.selectCases)
		if chosen == 0 {
			// closing.
			d.logger.Printf("zkdispatcher: exiting via close channel")
			return
		} else if chosen == 1 {
			// New handler
			nh := recv.Interface().(newHandler)
			d.selectHandlers = append(d.selectHandlers, nh.handler)
			d.selectCases = append(d.selectCases, newCase(nh.watcher))
			continue
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
	ret, ok := q.peek()
	if !ok {
		// remove item from map to make sure map does
		// not grow unbounded
		delete(d.tasks, cb)
		return zkDispatchTask{}, false
	}
	return ret.(zkDispatchTask), ok
}

func newCase(watcher <-chan zk.Event) reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(watcher)}
}
