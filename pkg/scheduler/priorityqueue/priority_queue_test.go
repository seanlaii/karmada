/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package priorityqueue_test

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karmada-io/karmada/pkg/scheduler/priorityqueue"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

func TestBasic(t *testing.T) {
	tests := []struct {
		queue         *priorityqueue.PriorityQueue
		queueShutDown func(workqueue.Interface)
	}{
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDown,
		},
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDownWithDrain,
		},
	}
	for _, test := range tests {
		// If something is seriously wrong this test will never complete.

		// Start producers
		const producers = 50
		producerWG := sync.WaitGroup{}
		producerWG.Add(producers)
		for i := 0; i < producers; i++ {
			go func(i int) {
				defer producerWG.Done()
				for j := 0; j < 50; j++ {
					test.queue.Add(priorityqueue.DataWithPriority{
						Key: strconv.Itoa(i),
					})
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Start consumers
		const consumers = 10
		consumerWG := sync.WaitGroup{}
		consumerWG.Add(consumers)
		for i := 0; i < consumers; i++ {
			go func(i int) {
				defer consumerWG.Done()
				for {
					item, quit := test.queue.Get()
					if item != nil {
						itemKey := item.(priorityqueue.DataWithPriority).Key
						if itemKey == "added after shutdown!" {
							t.Errorf("Got an item added after shutdown.")
						}
					}
					if quit {
						return
					}
					t.Logf("Worker %v: begin processing %v", i, item)
					time.Sleep(3 * time.Millisecond)
					t.Logf("Worker %v: done processing %v", i, item)
					test.queue.Done(item)
				}
			}(i)
		}

		producerWG.Wait()
		test.queueShutDown(test.queue)
		test.queue.Add(priorityqueue.DataWithPriority{
			Key: "added after shutdown!",
		})
		consumerWG.Wait()
		if test.queue.Len() != 0 {
			t.Errorf("Expected the queue to be empty, had: %v items", test.queue.Len())
		}
	}
}

func TestAddWhileProcessing(t *testing.T) {
	tests := []struct {
		queue         *priorityqueue.PriorityQueue
		queueShutDown func(workqueue.Interface)
	}{
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDown,
		},
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDownWithDrain,
		},
	}
	for _, test := range tests {

		// Start producers
		const producers = 50
		producerWG := sync.WaitGroup{}
		producerWG.Add(producers)
		for i := 0; i < producers; i++ {
			go func(i int) {
				defer producerWG.Done()
				test.queue.Add(priorityqueue.DataWithPriority{
					Key: strconv.Itoa(i),
				})
			}(i)
		}

		// Start consumers
		const consumers = 10
		consumerWG := sync.WaitGroup{}
		consumerWG.Add(consumers)
		for i := 0; i < consumers; i++ {
			go func(i int) {
				defer consumerWG.Done()
				// Every worker will re-add every item up to two times.
				// This tests the dirty-while-processing case.
				counters := map[interface{}]int{}
				for {
					item, quit := test.queue.Get()
					if quit {
						return
					}
					itemKey := item.(priorityqueue.DataWithPriority).Key
					counters[itemKey]++
					if counters[itemKey] < 2 {
						test.queue.Add(item)
					}
					test.queue.Done(item)
				}
			}(i)
		}

		producerWG.Wait()
		test.queueShutDown(test.queue)
		consumerWG.Wait()
		if test.queue.Len() != 0 {
			t.Errorf("Expected the queue to be empty, had: %v items", test.queue.Len())
		}
	}
}

func TestLen(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	if e, a := 1, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})
	if e, a := 2, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	}) // should not increase the queue length.
	if e, a := 2, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
}

func TestReinsert(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})

	// Start processing
	i, _ := q.Get()
	itemKey := i.(priorityqueue.DataWithPriority).Key
	if itemKey != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Add it back while processing
	q.Add(i)

	// Finish it up
	q.Done(i)

	// It should be back on the queue
	i, _ = q.Get()
	itemKey = i.(priorityqueue.DataWithPriority).Key
	if itemKey != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Finish that one up
	q.Done(i)

	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}
}

func TestCollapse(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	// Add a new one twice
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})

	// It should get the new one
	i, _ := q.Get()
	itemKey := i.(priorityqueue.DataWithPriority).Key
	if itemKey != "bar" {
		t.Errorf("Expected %v, got %v", "bar", i)
	}

	// Finish that one up
	q.Done(i)

	// There should be no more objects in the queue
	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}
}

func TestCollapseWhileProcessing(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})

	// Start processing
	i, _ := q.Get()
	itemKey := i.(priorityqueue.DataWithPriority).Key
	if itemKey != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Add the same one twice
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})

	waitCh := make(chan struct{})
	// simulate another worker consuming the queue
	go func() {
		defer close(waitCh)
		i, _ := q.Get()
		itemKey := i.(priorityqueue.DataWithPriority).Key
		if itemKey != "foo" {
			t.Errorf("Expected %v, got %v", "foo", i)
		}
		// Finish that one up
		q.Done(i)
	}()

	// give the worker some head start to avoid races
	// on the select statement that cause flakiness
	time.Sleep(100 * time.Millisecond)
	// Finish the first one to unblock the other worker
	select {
	case <-waitCh:
		t.Errorf("worker should be blocked until we are done")
	default:
		q.Done(priorityqueue.DataWithPriority{
			Key: "foo",
		})
	}

	// wait for the worker to consume the new object
	// There should be no more objects in the queue
	<-waitCh
	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}
}

func TestQueueDrainageUsingShutDownWithDrain(t *testing.T) {

	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})

	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})

	firstItem, _ := q.Get()
	secondItem, _ := q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// This is done as to simulate a sequence of events where ShutDownWithDrain
	// is called before we start marking all items as done - thus simulating a
	// drain where we wait for all items to finish processing.
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Mark the first two items as done, as to finish up
	q.Done(firstItem)
	q.Done(secondItem)

	finishedWG.Wait()
}

func TestNoQueueDrainageUsingShutDown(t *testing.T) {

	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})

	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})

	q.Get()
	q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		// Invoke ShutDown: suspending the execution immediately.
		q.ShutDown()
	}()

	// We can now do this and not have the test timeout because we didn't call
	// Done on the first two items before arriving here.
	finishedWG.Wait()
}

func TestForceQueueShutdownUsingShutDown(t *testing.T) {

	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})

	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	q.Add(priorityqueue.DataWithPriority{
		Key: "bar",
	})

	q.Get()
	q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// This is done as to simulate a sequence of events where ShutDownWithDrain
	// is called before ShutDown
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Use ShutDown to force the queue to shut down (simulating a caller
	// which can invoke this function on a second SIGTERM/SIGINT)
	q.ShutDown()

	// We can now do this and not have the test timeout because we didn't call
	// done on any of the items before arriving here.
	finishedWG.Wait()
}

func TestQueueDrainageUsingShutDownWithDrainWithDirtyItem(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})

	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})
	gotten, _ := q.Get()
	q.Add(priorityqueue.DataWithPriority{
		Key: "foo",
	})

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// Ensure that ShutDownWithDrain has started and is blocked.
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Finish "working".
	q.Done(gotten)

	// `shuttingDown` becomes false because Done caused an item to go back into
	// the queue.
	again, shuttingDown := q.Get()
	if shuttingDown {
		t.Fatalf("should not have been done")
	}
	q.Done(again)

	// Now we are really done.
	_, shuttingDown = q.Get()
	if !shuttingDown {
		t.Fatalf("should have been done")
	}

	finishedWG.Wait()
}

// TestGarbageCollection ensures that objects that are added then removed from the queue are
// able to be garbage collected.
func TestGarbageCollection(t *testing.T) {
	leakQueue := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	t.Cleanup(func() {
		// Make sure leakQueue doesn't go out of scope too early
		runtime.KeepAlive(leakQueue)
	})
	c := &priorityqueue.DataWithPriority{
		Key: "foo",
	}
	mustGarbageCollect(t, c)
	leakQueue.Add(*c)
	o, _ := leakQueue.Get()
	leakQueue.Done(o)
}

// mustGarbageCollect asserts than an object was garbage collected by the end of the test.
// The input must be a pointer to an object.
func mustGarbageCollect(t *testing.T, i interface{}) {
	t.Helper()
	var collected int32 = 0
	runtime.SetFinalizer(i, func(x interface{}) {
		atomic.StoreInt32(&collected, 1)
	})
	t.Cleanup(func() {
		if err := wait.PollImmediate(time.Millisecond*100, wait.ForeverTestTimeout, func() (done bool, err error) {
			// Trigger GC explicitly, otherwise we may need to wait a long time for it to run
			runtime.GC()
			return atomic.LoadInt32(&collected) == 1, nil
		}); err != nil {
			t.Errorf("object was not garbage collected")
		}
	})
}

func TestOrdering(t *testing.T) {
	tests := []struct {
		queue         *priorityqueue.PriorityQueue
		queueShutDown func(workqueue.Interface)
	}{
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDown,
		},
		{
			queue: priorityqueue.NewWithConfig(workqueue.QueueConfig{
				Name:  "test-queue",
				Clock: clock.RealClock{},
			}),
			queueShutDown: workqueue.Interface.ShutDownWithDrain,
		},
	}
	for _, test := range tests {
		counters := 50
		for i := 0; i < counters; i++ {
			test.queue.Add(priorityqueue.DataWithPriority{
				Key:      strconv.Itoa(i),
				Priority: counters - i,
			})
		}

		for i := 0; i < counters; i++ {
			item, _ := test.queue.Get()
			itemKey := item.(priorityqueue.DataWithPriority).Key
			priority := item.(priorityqueue.DataWithPriority).Priority
			intKey, _ := strconv.Atoi(itemKey)
			if intKey != i {
				t.Errorf("Expected itemKey equals %v, had: %v", counters-i, itemKey)
			}
			if priority != counters-i {
				t.Errorf("Expected priority equals %v, had: %v", counters-i, priority)
			}
			test.queue.Done(item)
		}

		test.queueShutDown(test.queue)
		if test.queue.Len() != 0 {
			t.Errorf("Expected the queue to be empty, had: %v items", test.queue.Len())
		}
	}
}

func TestPriorityWhileProcessing(t *testing.T) {
	q := priorityqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "test-queue",
		Clock: clock.RealClock{},
	})
	item1 := priorityqueue.DataWithPriority{
		Key:      strconv.Itoa(0),
		Priority: 1,
	}
	item1WithDifferentPriority := priorityqueue.DataWithPriority{
		Key:      strconv.Itoa(0),
		Priority: 2,
	}
	q.Add(item1)
	item, _ := q.Get()
	priority := item.(priorityqueue.DataWithPriority).Priority
	if priority != 1 {
		t.Errorf("Expected the priority to be 1, had: %v items", priority)
	}

	q.Add(item1)
	q.Add(item1WithDifferentPriority)

	q.Done(item)

	item, _ = q.Get()
	priority = item.(priorityqueue.DataWithPriority).Priority
	if priority != 2 {
		t.Errorf("Expected the priority to be 2, had: %v items", priority)
	}
	q.Done(item)

	workqueue.Interface.ShutDown(q)
	if q.Len() != 0 {
		t.Errorf("Expected the queue to be empty, had: %v items", q.Len())
	}
}
