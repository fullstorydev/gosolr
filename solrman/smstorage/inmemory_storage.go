// Copyright 2017 FullStory, Inc.
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

package smstorage

import (
	"sort"
	"sync"

	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
)

// Reference implementation for testing, don't use in production.
type InMemoryStorage struct {
	mu             sync.RWMutex
	disabled       bool
	splitsDisabled bool
	movesDisables  bool
	inProgress     map[string]solrmanapi.OpRecord
	completed      []solrmanapi.OpRecord
}

var _ SolrManStorage = &InMemoryStorage{}

func (s *InMemoryStorage) AddInProgressOp(op solrmanapi.OpRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inProgress == nil {
		s.inProgress = map[string]solrmanapi.OpRecord{}
	}
	s.inProgress[op.Key()] = op
	return nil
}

func (s *InMemoryStorage) DelInProgressOp(op solrmanapi.OpRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.inProgress, op.Key())
	return nil
}

func (s *InMemoryStorage) GetInProgressOps() ([]solrmanapi.OpRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ret []solrmanapi.OpRecord
	for _, op := range s.inProgress {
		ret = append(ret, op)
	}
	sort.Sort(solrmanapi.ByStartedRecently(ret))
	return ret, nil
}

func (s *InMemoryStorage) AddCompletedOp(op solrmanapi.OpRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.completed = append(s.completed, op)
	sort.Sort(solrmanapi.ByFinishedRecently(s.completed))
	if len(s.completed) > NumStoredCompletedOps {
		s.completed = s.completed[:NumStoredCompletedOps] // only keep the most recent NumStoredCompletedOps
	}
	return nil
}

func (s *InMemoryStorage) GetCompletedOps(count int) ([]solrmanapi.OpRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ret := s.completed
	if len(ret) > count {
		ret = ret[:count]
	}
	return ret, nil
}

func (s *InMemoryStorage) GetEvacuateNodeList() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return nil, nil
}

func (s *InMemoryStorage) IsDisabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.disabled
}

func (s *InMemoryStorage) SetDisabled(disabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.disabled = disabled
	return nil
}

func (s *InMemoryStorage) IsSplitsDisabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.splitsDisabled
}

func (s *InMemoryStorage) SetSplitsDisabled(disabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.splitsDisabled = disabled
	return nil
}

func (s *InMemoryStorage) IsMovesDisabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.movesDisables
}

func (s *InMemoryStorage) SetMovesDisabled(disabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.movesDisables = disabled
	return nil
}
