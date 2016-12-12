package solrmonitor

import (
	"github.com/samuel/go-zookeeper/zk"
	"k8s.io/kubernetes/staging/src/k8s.io/client-go/pkg/util/rand"
	"testing"
)

func TestSimple(t *testing.T) {
	q := fifoTaskQueue{}
	var g taskGenerator

	for i := 0; i < 10; i++ {
		task := g.newTask()
		q.add(task)
		if peeked, ok := q.peek(); !ok || !equals(peeked, task) {
			t.Error("Failed to peek task that was just added")
		}
		if polled, ok := q.poll(); !ok || !equals(polled, task) {
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

func TestRingBufferMaintenance(t *testing.T) {
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
	if int(task.event.Type) != *removeCount {
		t.Fatalf("Expecting to have polled %d; instead polled %d",
			*removeCount, int(task.event.Type))
	}
}

type taskGenerator int

func (g *taskGenerator) newTask() zkDispatchTask {
	(*g)++
	return zkDispatchTask{
		callback: &zkEventHandlerAdapter{func(zk.Event) <-chan zk.Event {
			return nil
		}},
		event: zk.Event{Type: zk.EventType(*g)},
	}
}
