package solrmonitor

import "github.com/samuel/go-zookeeper/zk"

// zkDispatchTask is a queued task
type zkDispatchTask struct {
	callback ZkEventCallback
	event    zk.Event
}

// fifoTaskQueue is a FIFO queue using a ring buffer. A worker goroutine processes elements in
// a queue. This uses a ring buffer instead of a simpler approach that only uses Go slices
// (e.g. q = append(q, e) to enqueue; q = q[1:] to dequeue). The simpler approach incurs many more
// re-allocations of the slice's underlying array whereas the ring buffer approach only grows the
// array when necessary to store the whole of actively enqueued elements.
type fifoTaskQueue struct {
	slice []zkDispatchTask
	tail  int
	head  int
	size  int
}

// add adds a new item to the queue, potentially growing the underlying ring buffer if necessary.
func (q *fifoTaskQueue) add(task zkDispatchTask) {
	if q.head == q.tail {
		if len(q.slice) == 0 {
			q.slice = make([]zkDispatchTask, 16)
		} else if q.size > 0 {
			// Grow underlying slice. We can't rely on Go's append() for adding to the
			// queue because that would lead to unbounded storage as the offset is
			// continually incremented with each poll. So we use a circular buffer, and
			// thus must manage the growing of the slice ourselves
			var biggerSlice []zkDispatchTask
			newSize := len(q.slice) * 2
			if newSize <= 0 {
				// overflow really should never happen, but out of an
				// abundance of caution...
				newSize = maxInt
				if q.size >= newSize {
					panic("cannot grow queue any further!")
				}
			}
			biggerSlice = make([]zkDispatchTask, newSize)
			leadingSlice, trailingSlice := q.slice[q.head:], q.slice[:q.head]
			copy(biggerSlice, leadingSlice)
			copy(biggerSlice[len(leadingSlice):], trailingSlice)
			q.head = 0
			q.tail = q.size
			q.slice = biggerSlice
		}
	}
	q.slice[q.tail] = task
	q.tail = (q.tail + 1) % len(q.slice)
	q.size++
}

// pop removes and returns the next item in the queue.
func (q *fifoTaskQueue) poll() (polled zkDispatchTask, ok bool) {
	if q.size == 0 {
		ok = false
		return
	} else {
		polled = q.slice[q.head]
		ok = true
	}
	q.head = (q.head + 1) % len(q.slice)
	q.size--
	return
}

// peek returns the head of the queue. The ok flag will be false if the queue is empty.
func (q *fifoTaskQueue) peek() (head zkDispatchTask, ok bool) {
	if q.size == 0 {
		ok = false
	} else {
		head = q.slice[q.head]
		ok = true
	}
	return
}
