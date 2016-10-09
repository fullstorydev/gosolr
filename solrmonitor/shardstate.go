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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
)

type ShardState struct {
	Range    string                  `json:"range"` // e.g. "80000000-b332ffff"
	Replicas map[string]ReplicaState `json:"replicas"`
	State    string                  `json:"state"` // e.g. "active", "inactive"

	cachedBounds atomic.Value // of (*bounds)
}

type bounds struct {
	low int32
	hi  int32
	err error
}

// HashRangeShare returns the faction (from 0 to 1) of the total hash range (of the shard's collection) that this shard
// is responsible for.
func (s ShardState) HashRangeShare() float64 {
	low, high, err := s.RangeBounds()
	if err != nil {
		return 0
	}

	// Use int64 to hold differences larger than 32 bits can hold.
	used := int64(high) - int64(low)
	return float64(used) / 0x100000000
}

func (s ShardState) IsActive() bool {
	return s.State == "active"
}

func (s ShardState) RangeBounds() (int32, int32, error) {
	var cachedBounds *bounds
	v := s.cachedBounds.Load()
	if v == nil {
		// Don't need an actual lock here because computeBounds() safe to compute+store multiple times.
		cachedBounds = computeBounds(s.Range)
		s.cachedBounds.Store(cachedBounds)
	} else {
		cachedBounds = v.(*bounds)
	}
	return cachedBounds.low, cachedBounds.hi, cachedBounds.err
}

// Parse as uint, then cast to int32 to force wrapping into range.  This is what Solr does.
func computeBounds(rangeStr string) *bounds {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return &bounds{err: fmt.Errorf("failed to split %q", rangeStr)}
	}

	low, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil || low > math.MaxUint32 {
		return &bounds{err: fmt.Errorf("failed to parse %q", parts[0])}
	}

	high, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil || high > math.MaxUint32 {
		return &bounds{err: fmt.Errorf("failed to parse %q", parts[1])}
	}

	if int32(low) > int32(high) {
		return &bounds{err: fmt.Errorf("low should be <= high %q", rangeStr)}
	}

	return &bounds{low: int32(low), hi: int32(high)}
}
