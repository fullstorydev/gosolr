// Copyright 2016 FullStory, Inc.
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

package solrmonitor

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type ShardState struct {
	Parent   string                  `json:"parent"`
	Range    string                  `json:"range"` // e.g. "80000000-b332ffff"
	State    string                  `json:"state"` // e.g. "active", "inactive"
	Replicas map[string]ReplicaState `json:"replicas,omitempty"`

	rangeBounds      bounds
	rangeInitialized bool
}

type bounds struct {
	lo  int32
	hi  int32
	err error
}

func (s ShardState) IsActive() bool {
	return s.State == "active"
}

func (s ShardState) RangeBounds() (int32, int32, error) {
	var ret bounds
	if s.rangeInitialized {
		ret = s.rangeBounds
	} else {
		ret = computeBounds(s.Range)
	}
	return ret.lo, ret.hi, ret.err
}

func (s ShardState) WithRangeBounds() ShardState {
	s.rangeBounds = computeBounds(s.Range)
	s.rangeInitialized = true
	return s
}

func (s *ShardState) UnmarshalJSON(data []byte) error {
	// Use type alias to avoid infinite unmarshal recursion
	type Alias ShardState
	if err := json.Unmarshal(data, (*Alias)(s)); err != nil {
		return err
	}

	s.rangeBounds = computeBounds(s.Range)
	s.rangeInitialized = true
	return nil
}

// If a shard only has one host, returns that host.
func (s *ShardState) FindSingleHostForShard() string {
	if len(s.Replicas) == 1 {
		for _, replica := range s.Replicas {
			return replica.BaseUrl
		}
	}
	return ""
}

// Parse as uint, then cast to int32 to force wrapping into range.  This is what Solr does.
func computeBounds(rangeStr string) bounds {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return bounds{err: fmt.Errorf("failed to split %q", rangeStr)}
	}

	lo, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil || lo > math.MaxUint32 {
		return bounds{err: fmt.Errorf("failed to parse %q", parts[0])}
	}

	hi, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil || hi > math.MaxUint32 {
		return bounds{err: fmt.Errorf("failed to parse %q", parts[1])}
	}

	if int32(lo) > int32(hi) {
		return bounds{err: fmt.Errorf("low should be <= high %q", rangeStr)}
	}

	return bounds{lo: int32(lo), hi: int32(hi)}
}
