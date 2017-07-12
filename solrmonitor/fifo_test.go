package solrmonitor

import (
	"math/rand"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

func TestFifoSimple(t *testing.T) {
	q := fifoTaskQueue{}
	var g taskGenerator

	for i := 0; i < 10; i++ {
		task := g.newTask()
		q.add(task)
		if peeked, ok := q.peek(); !ok || !equals(peeked.(zkDispatchTask), task) {
			t.Error("Failed to peek task that was just added")
		}
		if polled, ok := q.poll(); !ok || !equals(polled.(zkDispatchTask), task) {
			t.Error("Failed to poll task that was just added")
		}

		// queue is now empty
		if _, ok := q.peek(); ok {
			t.Error("Peek should have failed as queue should now be empty")
		}
		if _, ok := q.poll(); ok {
			t.Error("Poll should have failed as queue should now be empty")
		}
	}
}

func TestFifoRingBufferMaintenance(t *testing.T) {
	// We do lots of operations to make sure we test various cases, like resizing of the
	// queues buffer, head and tail wrapping past the end of the ring buffer, etc.

	q := &fifoTaskQueue{}
	var g taskGenerator
	removed := 0

	// fill the queue and get it to grow (occasional removals to make sure it works
	// when the head pointer is not at the beginning of the slice)
	for q.size < 1000 {
		if rand.Intn(5) == 0 && q.size > 0 {
			checkRemove(q, t, &removed)
		} else {
			q.add(g.newTask())
		}
	}

	// add and remove, to make sure we wrap head and tail around the end of the buffer
	for i := 0; i < 10*1000; i++ {
		if i%2 == 0 {
			q.add(g.newTask())
		} else {
			checkRemove(q, t, &removed)
		}
	}

	// finally, drain the queue
	for q.size > 0 {
		checkRemove(q, t, &removed)
	}

	// ensure that we don't leak any references in underlying slice after removing from queue
	for idx, task := range q.slice {
		if task != nil {
			t.Errorf("Entry in queue at index %d was not cleared: %v", idx, task)
		}
	}
}

func equals(task1, task2 zkDispatchTask) bool {
	// function types are not comparable; we really only need to care about the events
	return task1.event == task2.event
}

func checkRemove(q *fifoTaskQueue, t *testing.T, removeCount *int) {
	task, ok := q.poll()
	if !ok {
		t.Fatalf("Polling from queue failed even though size = %d", q.size)
	}
	(*removeCount)++
	if int(task.(zkDispatchTask).event.Type) != *removeCount {
		t.Fatalf("Expecting to have polled %d; instead polled %d",
			*removeCount, int(task.(zkDispatchTask).event.Type))
	}
}

type taskGenerator int

func (g *taskGenerator) newTask() zkDispatchTask {
	(*g)++
	handler := ZkEventHandler(func(zk.Event) <-chan zk.Event {
		return nil
	})
	return zkDispatchTask{
		callback: &handler,
		event:    zk.Event{Type: zk.EventType(*g)},
	}
}
