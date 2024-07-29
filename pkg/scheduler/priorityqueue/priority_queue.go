package priorityqueue

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type PiroirtyQueue struct {
	queue      *Heap
	dirty      Set
	processing Set
	cond       *sync.Cond

	shuttingDown bool
	drain        bool
	metrics      workqueue.queueMetrics

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.WithTicker
}

type empty struct{}
type t interface{}

type Set struct {
	keySet  map[string]empty
	keyFunc KeyFunc
}

func (s Set) has(item t) bool {
	key, _ := s.keyFunc(item)
	_, exisits := s.keySet[key]
	return exisits
}

func (s Set) insert(item t) {
	key, _ := s.keyFunc(item)
	s.keySet[key] = empty{}
}

func (s Set) delete(item t) {
	key, _ := s.keyFunc(item)
	delete(s.keySet, key)
}

func (s Set) len() int {
	return len(s.keySet)
}
