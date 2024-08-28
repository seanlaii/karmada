/*
Copyright 2022 The Karmada Authors.

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

package priorityqueue

import (
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

type DataWithPriority struct {
	Key      string
	Priority int
}

// NewWithConfig constructs a new workqueue with ability to
// customize different properties.
func NewWithConfig(config workqueue.QueueConfig) *PriorityQueue {
	return newQueueWithConfig(config, defaultUnfinishedWorkUpdatePeriod)
}

// newQueueWithConfig constructs a new named workqueue
// with the ability to customize different properties for testing purposes
func newQueueWithConfig(config workqueue.QueueConfig, updatePeriod time.Duration) *PriorityQueue {
	var metricsFactory *queueMetricsFactory
	if config.MetricsProvider != nil {
		metricsFactory = &queueMetricsFactory{
			metricsProvider: config.MetricsProvider,
		}
	} else {
		metricsFactory = &globalMetricsFactory
	}

	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}

	return newQueue(
		config.Clock,
		metricsFactory.newQueueMetrics(config.Name, config.Clock),
		updatePeriod,
	)
}

func newQueue(c clock.WithTicker, metrics queueMetrics, updatePeriod time.Duration) *PriorityQueue {
	t := &PriorityQueue{
		clock: c,
		queue: New(
			func(obj interface{}) (string, error) {
				return obj.(DataWithPriority).Key, nil
			},
			func(item1, item2 interface{}) bool {
				return item1.(DataWithPriority).Priority > item2.(DataWithPriority).Priority
			},
		),
		dirty: Set{
			keySet: make(map[string]t),
			keyFunc: func(obj interface{}) (string, error) {
				return obj.(DataWithPriority).Key, nil
			},
		},
		processing: Set{
			keySet: make(map[string]t),
			keyFunc: func(obj interface{}) (string, error) {
				return obj.(DataWithPriority).Key, nil
			},
		},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
	}

	// Don't start the goroutine for a type of noMetrics so we don't consume
	// resources unnecessarily
	if _, ok := metrics.(noMetrics); !ok {
		go t.updateUnfinishedWorkLoop()
	}

	return t
}

const defaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond

type PriorityQueue struct {
	queue *Heap

	// dirty defines all of the items that need to be processed.
	dirty Set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing Set
	cond       *sync.Cond

	shuttingDown bool
	drain        bool
	metrics      queueMetrics

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.WithTicker
}

type t interface{}

type Set struct {
	keySet  map[string]t
	keyFunc KeyFunc
}

func (s Set) has(item t) bool {
	key, _ := s.keyFunc(item)
	_, exisits := s.keySet[key]
	return exisits
}

func (s Set) insert(item t) {
	key, _ := s.keyFunc(item)
	s.keySet[key] = item
}

func (s Set) delete(item t) {
	key, _ := s.keyFunc(item)
	delete(s.keySet, key)
}

func (s Set) get(item t) t {
	key, _ := s.keyFunc(item)
	return s.keySet[key]
}

func (s Set) len() int {
	return len(s.keySet)
}

// Add marks item as needing processing.
func (q *PriorityQueue) Add(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		if !q.processing.has(item) {
			q.queue.Update(item)
		} else {
			q.dirty.delete(item)
			q.dirty.insert(item)
		}
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	q.queue.Add(item)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *PriorityQueue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *PriorityQueue) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.queue.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.queue.Len() == 0 {
		return *new(interface{}), true
	}

	item, _ = q.queue.Pop()

	q.metrics.get(item)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}

func (q *PriorityQueue) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	q.processing.delete(item)
	if q.dirty.has(item) {
		itemFromDirty := q.dirty.get(item)
		q.queue.Add(itemFromDirty)
		q.cond.Signal()
	} else if q.processing.len() == 0 {
		q.cond.Signal()
	}
}

func (q *PriorityQueue) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = false
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *PriorityQueue) ShutDownWithDrain() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = true
	q.shuttingDown = true
	q.cond.Broadcast()

	for q.processing.len() != 0 && q.drain {
		q.cond.Wait()
	}
}

func (q *PriorityQueue) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

func (q *PriorityQueue) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}
