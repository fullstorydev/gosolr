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
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

// An adapter around testing.T that allows finer grain control of stack frame logging.
// Generally you would wrap this in another type that exposes the desired logging interface.
type TestLogger interface {
	// Logs the given message to the test log, skipping the desired number of stack frames.
	DoLog(s string, skipFrames int)
}

// Adapt a testing.T to expose its inner logging with better stack frame handling.
func AdaptTestLogger(t *testing.T) TestLogger {
	// Just cast the internal type to our look-alike type.
	return (*logger)(unsafe.Pointer(t))
}

// Lookalike copy of testing.T/testing.common, so we can access the bits directly.
// Sadly, there's no other way to get the right call stack depth handling.
type logger struct {
	mu     sync.RWMutex
	output []byte
}

func (t *logger) DoLog(s string, skipFrames int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msg := t.decorate(s, skipFrames+1)
	t.output = append(t.output, msg...)
}

// decorate prefixes the string with the file and line of the call site
// and inserts the final newline if needed and indentation tabs for formatting.
// Almost a direct copy of the internal testing.common -> decorate()
func (t *logger) decorate(s string, skipFrames int) string {
	_, file, line, ok := runtime.Caller(skipFrames) // decorate + log + public function.
	if ok {
		// Truncate file name at last file name separator.
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		} else if index = strings.LastIndex(file, "\\"); index >= 0 {
			file = file[index+1:]
		}
	} else {
		file = "???"
		line = 1
	}
	buf := new(bytes.Buffer)
	// Every line is indented at least one tab.
	buf.WriteByte('\t')
	fmt.Fprintf(buf, "%s:%d: ", file, line)
	lines := strings.Split(s, "\n")
	if l := len(lines); l > 1 && lines[l-1] == "" {
		lines = lines[:l-1]
	}
	for i, line := range lines {
		if i > 0 {
			// Second and subsequent lines are indented an extra tab.
			buf.WriteString("\n\t\t")
		}
		buf.WriteString(line)
	}
	buf.WriteByte('\n')
	return buf.String()
}
