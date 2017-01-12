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
	"errors"
	"math"
	"sync"
	"testing"
)

func TestComputeBounds(t *testing.T) {
	assertBounds(t, &bounds{low: math.MinInt32, hi: math.MaxInt32}, computeBounds("80000000-7fffffff"))
	assertBounds(t, &bounds{low: 0, hi: 0x6eeeeee}, computeBounds("00000000-06eeeeee"))
	assertBounds(t, &bounds{low: 0, hi: 0x5dddddd}, computeBounds("0-5dddddd"))
	assertBounds(t, &bounds{low: -1, hi: 0}, computeBounds("ffffffff-0"))
	assertBounds(t, &bounds{low: 3, hi: 3}, computeBounds("3-3"))
	assertBounds(t, &bounds{err: errors.New(`failed to split "omg"`)}, computeBounds("omg"))
	assertBounds(t, &bounds{err: errors.New(`failed to parse "wtf"`)}, computeBounds("80000000-wtf"))
	assertBounds(t, &bounds{err: errors.New(`failed to parse "lol"`)}, computeBounds("lol-7fffffff"))
	assertBounds(t, &bounds{err: errors.New(`low should be <= high "4-0"`)}, computeBounds("4-0"))
}

func TestCachedBoundsRace(t *testing.T) {
	shard := &ShardState{Range: "0-4cccccc"}

	numRoutines := 1024
	var start sync.WaitGroup
	var end sync.WaitGroup
	start.Add(numRoutines)
	end.Add(numRoutines)

	for i := 0; i < numRoutines; i++ {
		go func() {
			defer end.Done()
			start.Done()
			start.Wait()

			actual := &bounds{}
			actual.low, actual.hi, actual.err = shard.RangeBounds()
			assertBounds(t, &bounds{low: 0, hi: 0x4cccccc}, actual)
		}()
	}

	end.Wait()
}

func assertBounds(t *testing.T, expected *bounds, actual *bounds) {
	if expected.err != nil {
		if actual.err == nil {
			t.Errorf("Expected err: %s, actual: nil", expected.err.Error())
		} else if expected.err.Error() != actual.err.Error() {
			t.Errorf("Expected err: %s, actual: %s", expected.err.Error(), actual.err.Error())
		}
		return
	}

	if actual.err != nil {
		t.Errorf("Expected no err, actual: %s", actual.err.Error())
		return
	}

	if expected.low != actual.low {
		t.Errorf("Expected low: %d, actual: %d", expected.low, actual.low)
	}

	if expected.hi != actual.hi {
		t.Errorf("Expected hi: %d, actual: %d", expected.hi, actual.hi)
	}
}
