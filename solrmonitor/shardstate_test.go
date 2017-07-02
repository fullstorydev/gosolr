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
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestComputeBounds(t *testing.T) {
	assertBounds(t, bounds{lo: math.MinInt32, hi: math.MaxInt32}, computeBounds("80000000-7fffffff"))
	assertBounds(t, bounds{lo: 0, hi: 0x6eeeeee}, computeBounds("00000000-06eeeeee"))
	assertBounds(t, bounds{lo: 0, hi: 0x5dddddd}, computeBounds("0-5dddddd"))
	assertBounds(t, bounds{lo: -1, hi: 0}, computeBounds("ffffffff-0"))
	assertBounds(t, bounds{lo: 3, hi: 3}, computeBounds("3-3"))
	assertBounds(t, bounds{err: errors.New(`failed to split "omg"`)}, computeBounds("omg"))
	assertBounds(t, bounds{err: errors.New(`failed to parse "wtf"`)}, computeBounds("80000000-wtf"))
	assertBounds(t, bounds{err: errors.New(`failed to parse "lol"`)}, computeBounds("lol-7fffffff"))
	assertBounds(t, bounds{err: errors.New(`low should be <= high "4-0"`)}, computeBounds("4-0"))
}

func TestUnmarshal(t *testing.T) {
	tcs := []ShardState{
		{Parent: "parent", Range: "80000000-7fffffff", State: "active"},
		{Parent: "parent", Range: "80000000-b332ffff", State: "active"},
		{Parent: "parent", Range: "80000000-wtf", State: "active"},
		{Parent: "parent", Range: "lol-7fffffff", State: "active"},
		{Parent: "parent", Range: "4-0", State: "active"},
	}

	for i := range tcs {
		// bounds not precomputed
		original := tcs[i]

		expected := original.WithRangeBounds()

		var actual ShardState
		jsonUnmarshal(jsonMarshal(&original), &actual)

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("case %d; expected: %+v, actual: %+v", i, expected, actual)
		}

		assertBounds(t, rangeBounds(expected), rangeBounds(original))
		assertBounds(t, rangeBounds(expected), rangeBounds(actual))

		// Sanity check the internal values
		if original.rangeInitialized {
			t.Error("expected !original.rangeInitialized")
		}
		if !expected.rangeInitialized {
			t.Error("expected expected.rangeInitialized")
		}
		if !actual.rangeInitialized {
			t.Error("expected actual.rangeInitialized")
		}
	}
}

func rangeBounds(v ShardState) bounds {
	var ret bounds
	ret.lo, ret.hi, ret.err = v.RangeBounds()
	return ret
}

func assertBounds(t *testing.T, expected bounds, actual bounds) {
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

	if expected.lo != actual.lo {
		t.Errorf("Expected low: %d, actual: %d", expected.lo, actual.lo)
	}

	if expected.hi != actual.hi {
		t.Errorf("Expected hi: %d, actual: %d", expected.hi, actual.hi)
	}
}

func jsonMarshal(val interface{}) string {
	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		panic(err.Error())
	}
	return string(b)
}

func jsonUnmarshal(data string, val interface{}) {
	err := json.Unmarshal([]byte(data), val)
	if err != nil {
		panic(err.Error())
	}
}
