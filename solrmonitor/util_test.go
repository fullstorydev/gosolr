package solrmonitor

import (
	"testing"
	"time"
)

// Utility functions used by tests

const (
	checkTimeout  = 3 * time.Second
	checkInterval = 10 * time.Millisecond
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

func shouldExist(t *testing.T, sm *SolrMonitor, name string) {
	t.Helper()

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
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %s to exist, but it does not", name)
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
	t.Errorf("expected %s to not exist, but it does", name)
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
