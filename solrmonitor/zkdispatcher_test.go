package solrmonitor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// simple generator of zk.Event objects, with monotonically increasing Type field
type eventGenerator int32

func (g *eventGenerator) newEvent() zk.Event {
	e := atomic.AddInt32((*int32)(g), 1)
	return zk.Event{Type: zk.EventType(e)}
}

// state used and tracked throughout each test case
type testState struct {
	t *testing.T
	// generator of events, for submitting an event to fire the re-registered watch
	gen eventGenerator
	// completes when the callback is done and no longer re-registering itself
	done sync.WaitGroup
	// number of concurrent executions
	numConcurrentExecutions int32
	// maximum number of concurrent executions observed
	maxConcurrentExecutions int32
	// optional wait group used to let watcher go routines start
	ready *sync.WaitGroup
	// optional wait group used to coordinate the start of handling
	run *sync.WaitGroup
	// if true, assume events are consumed perfectly in order (e.g. event #1 is the first
	// event handled, event #2 is the second, and so on)
	assumeEventOrder bool
}

func (state *testState) newCallback(maxEvents int32) *testCallback {
	if state.ready != nil {
		state.ready.Add(1)
	}
	return &testCallback{
		state:        state,
		maxNumEvents: maxEvents,
		ready:        state.ready,
		run:          state.run,
	}
}

// callback for testing dispatches
type testCallback struct {
	state *testState
	// number of events processed
	numEvents int32
	// maximum number of events to process, after which the callback no longer re-registers
	// itself
	maxNumEvents int32
	// if non-nil, mark this as done at the start of first callback (to indicate that the
	// callback is ready to start)
	ready *sync.WaitGroup
	// if non-nil, wait on this before starting first callback (to coordinate the start of
	// multiple callbacks)
	run *sync.WaitGroup
}

func (cb *testCallback) Handle(e zk.Event) <-chan zk.Event {
	// track the maximum number of concurrent executions
	num := atomic.AddInt32(&cb.state.numConcurrentExecutions, 1)
	defer atomic.AddInt32(&cb.state.numConcurrentExecutions, -1)

	// if these wait groups are non-nil, we are coordinating the start of handling
	if cb.ready != nil {
		cb.ready.Done()
		cb.ready = nil
	}
	if cb.run != nil {
		cb.run.Wait()
		cb.run = nil
	}

	for {
		max := atomic.LoadInt32(&cb.state.maxConcurrentExecutions)
		if num <= max {
			break
		}
		if num > max &&
			atomic.CompareAndSwapInt32(&cb.state.maxConcurrentExecutions, max, num) {
			break
		}
	}

	numEvents := atomic.AddInt32(&cb.numEvents, 1)
	if cb.state.assumeEventOrder && e.Type != zk.EventType(numEvents) {
		cb.state.t.Errorf("Expecting event %d, got %d", numEvents, int(e.Type))
	}

	if numEvents >= cb.maxNumEvents {
		cb.state.done.Done()
		return nil
	} else {
		ch := make(chan zk.Event, 1)
		ch <- cb.state.gen.newEvent()
		return ch
	}
}

func TestReregistration(t *testing.T) {
	disp := NewZkDispatcher(zk.DefaultLogger)

	state := testState{t: t, assumeEventOrder: true}
	cb := state.newCallback(100)
	state.done.Add(1)

	channel := make(chan zk.Event, 1)
	if err := disp.WatchEvent(channel, cb); err != nil {
		t.Fatalf("failed to watch: %s", err)
	}

	channel <- state.gen.newEvent()
	state.done.Wait()

	if cb.numEvents != 100 {
		t.Errorf("Should have processed 100 events but instead processed %d", cb.numEvents)
	}

	tasksShouldBecomeEmpty(t, disp)
}

func TestDispatchesToSameCallbackAreSerial(t *testing.T) {
	disp := NewZkDispatcher(zk.DefaultLogger)
	var channels [10]chan zk.Event
	var run sync.WaitGroup
	run.Add(1)

	state := testState{t: t, run: &run}
	cb := state.newCallback(10)

	for i := 0; i < 10; i++ {
		state.done.Add(1)
		channels[i] = make(chan zk.Event, 1)
		if err := disp.WatchEvent(channels[i], cb); err != nil {
			t.Fatalf("failed to watch: %s", err)
		}
	}

	// send an event to get them started
	for i := 0; i < 10; i++ {
		channels[i] <- state.gen.newEvent()
	}
	// give dispatch go routine(s) a chance to start
	time.Sleep(200 * time.Millisecond)
	// let 'em rip
	run.Done()

	// wait for them all to finish
	state.done.Wait()

	if state.maxConcurrentExecutions != 1 {
		t.Errorf("Executions should be serial, but detected %d concurrent exceutions",
			state.maxConcurrentExecutions)
	}

	tasksShouldBecomeEmpty(t, disp)
}

func TestDispatchesToSameNakedFuncAreConcurrent(t *testing.T) {
	disp := NewZkDispatcher(zk.DefaultLogger)
	var channels [10]chan zk.Event
	var run sync.WaitGroup
	run.Add(1)

	state := testState{t: t, run: &run}
	cb := state.newCallback(10)
	f := cb.Handle

	for i := 0; i < 10; i++ {
		state.done.Add(1)
		channels[i] = make(chan zk.Event, 1)
		disp.Watch(channels[i], f)
	}

	// send an event to get them started
	for i := 0; i < 10; i++ {
		channels[i] <- state.gen.newEvent()
	}
	// give dispatch go routine(s) a chance to start
	time.Sleep(200 * time.Millisecond)
	// let 'em rip
	run.Done()

	// wait for them all to finish
	state.done.Wait()

	if state.maxConcurrentExecutions != 10 {
		t.Errorf("Should have been 10 concurrent executions (1 for each lambda);"+
			" instead detected %d", state.maxConcurrentExecutions)
	}

	tasksShouldBecomeEmpty(t, disp)
}

func TestDispatchesToDifferentCallbacksAreConcurrent(t *testing.T) {
	disp := NewZkDispatcher(zk.DefaultLogger)
	var channels [10]chan zk.Event
	var run, ready sync.WaitGroup
	run.Add(1)

	state := testState{t: t, run: &run, ready: &ready}
	var callbacks [10]*testCallback
	for i := 0; i < 10; i++ {
		callbacks[i] = state.newCallback(10)
		state.done.Add(1)
		channels[i] = make(chan zk.Event, 1)
		if err := disp.WatchEvent(channels[i], callbacks[i]); err != nil {
			t.Fatalf("failed to watch: %s", err)
		}
	}

	// send an event to get them started
	for i := 0; i < 10; i++ {
		channels[i] <- state.gen.newEvent()
	}

	// make sure they are all ready
	ready.Wait()
	// let 'em rip
	run.Done()

	// wait for them all to finish
	state.done.Wait()

	if state.maxConcurrentExecutions != 10 {
		t.Errorf("Should have been 10 concurrent executions (1 for each callback);"+
			" instead detected %d", state.maxConcurrentExecutions)
	}

	tasksShouldBecomeEmpty(t, disp)
}

func tasksShouldBecomeEmpty(t *testing.T, disp *ZkDispatcher) {
	// make sure that map of tasks gets cleaned up and returns to empty
	shouldBecomeEq(t, 0, func() int32 {
		disp.taskMu.Lock()
		defer disp.taskMu.Unlock()
		return int32(len(disp.tasks))
	})
}
