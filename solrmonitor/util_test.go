package solrmonitor

import (
	"sync/atomic"
	"testing"
	"time"
)

// Utility functions used by tests

const (
	checkTimeout  = 3 * time.Second
	checkInterval = 500 * time.Millisecond
)

func shouldBecomeEq(t *testing.T, expected int32, actualFunc func() int32) {
	t.Helper()

	var actual int32
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		actual = actualFunc()
		if expected == actual {
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %d, got: %d", expected, actual)
}

func shouldExist(t *testing.T, sm *SolrMonitor, name string, assert func(collectionState *CollectionState) error) {
	t.Helper()

	var assertErr error
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		collectionState, err := sm.GetCollectionState(name)
		if err != nil {
			t.Fatal(err)
			return
		}
		if collectionState != nil {
			// GetCurrentState should be consistent
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val == nil || !ok {
				t.Errorf("expected %s to exist in state map, but it does not", name)
			}
			assertErr = assert(val)
			if assertErr == nil {
				return // success
			}
		}
		time.Sleep(checkInterval)
	}
	t.Fatalf("expected %s to exist / match assertions, but it does not, assert err: %s", name, assertErr)
}

func prsShouldExist(t *testing.T, sm *SolrMonitor, name string, shard string, replica string, rstate string, leader string, version int32) {
	t.Helper()

	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		time.Sleep(checkInterval)
		collectionState, err := sm.GetCollectionState(name)
		if err != nil {
			t.Fatal(err)
			return
		}
		if collectionState != nil {
			// GetCurrentState should be consistent
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val == nil || !ok {
				t.Errorf("expected %s to exist in state map, but it does not", name)
				continue
			}

			if !val.isPRSEnabled() {
				t.Errorf("expected collection %s to be PRS", name)
				continue
			}

			s, sfound := val.Shards[shard]
			if !sfound {
				t.Errorf("expected collection %s shard %s to be exist", name, shard)
				continue
			}

			r, rfound := s.Replicas[replica]
			if !rfound {
				t.Errorf("expected shard %s 's, replica %s to be exist", shard, replica)
				continue
			}

			if r.State != rstate || r.Leader != leader || r.Version != version {
				t.Errorf("expected replica [%v] should be state[%s], leader[%s] and version[%d] ", r, rstate, leader, version)
				continue
			}

			return // success
		}
	}

	t.Fatalf("expected collection %s 's replica updated", name)
}

func checkFetchCount(t *testing.T, currentFetchCount *atomic.Int32, expectedFetchCount int32) {
	t.Helper()
	// Wait a moment before checking, otherwise it might not flag when there are too many fetches
	time.Sleep(200 * time.Millisecond)
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		if currentFetchCount.Load() > expectedFetchCount {
			t.Fatalf("fetch count %v exceeded expected count %v", currentFetchCount.Load(), expectedFetchCount)
			return
		}
		if currentFetchCount.Load() == expectedFetchCount {
			return
		}
		time.Sleep(checkInterval)
	}
	t.Fatalf("fetch count %v is not equal to expected count %v", currentFetchCount.Load(), expectedFetchCount)
}

func shouldNotExist(t *testing.T, sm *SolrMonitor, name string) {
	t.Helper()

	// Wait a moment before checking so we don't false-positive.
	time.Sleep(200 * time.Millisecond)
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		collectionState, err := sm.GetCollectionState(name)
		if err != nil {
			t.Fatal(err)
			return
		}
		if collectionState == nil {
			// GetCurrentState should be consistent
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val != nil || ok {
				t.Errorf("expected %s to not exist in state map, but it does", name)
			}
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Fatalf("expected %s to not exist, but it does", name)
}

func shouldError(t *testing.T, sm *SolrMonitor, name string) {
	t.Helper()

	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		_, err := sm.GetCollectionState(name)
		if err != nil {
			// GetCurrentState should only silently record an error, however
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val != nil || ok {
				t.Errorf("expected %s to not exist in state map, but it does", name)
			}
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %s to error, but no error", name)
}
