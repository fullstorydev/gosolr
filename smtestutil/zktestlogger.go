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

package smtestutil

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/go-zookeeper/zk"
)

// Adapts TestLogger to the ZK logging interface. captures errors.
func NewZkTestLogger(t *testing.T) *ZkTestLogger {
	return &ZkTestLogger{
		testLogger: AdaptTestLogger(t),
	}
}

// Adapts TestLogger to the ZK logging interface. captures errors.
type ZkTestLogger struct {
	mu         sync.RWMutex
	testLogger TestLogger
	errors     []error
}

var _ zk.Logger = &ZkTestLogger{}

func (l *ZkTestLogger) Printf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, arg := range args {
		err, isError := arg.(error)
		if isError && err != io.EOF {
			l.errors = append(l.errors, err)
		}
	}
	l.testLogger.DoLog(fmt.Sprintf(format, args...), 2)
}

func (l *ZkTestLogger) AssertNoErrors(t *testing.T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, err := range l.errors {
		t.Errorf("Error was logged %s", err)
	}
}

func (l *ZkTestLogger) GetErrorCount() int32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int32(len(l.errors))
}

func (l *ZkTestLogger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = nil
}
