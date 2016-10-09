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

package smservice

import (
	"fmt"
	"runtime"
)

// TODO: replace this with https://github.com/pkg/errors

const (
	lineSeparator = "\n...caused by "
)

type chainedError struct {
	cause error
	file  string
	line  int
	msg   string
}

// Returns the cause of this error, which may be nil
func (err *chainedError) Cause() error {
	return err.cause
}

// Returns the cause of this error, which may be nil
func (err *chainedError) Root() error {
	if err.cause == nil {
		return err
	}

	if causeIsChain, ok := err.cause.(*chainedError); ok {
		return causeIsChain.Root()
	}

	return err.cause
}

// Error returns a string describing the entire causal chain.
func (err *chainedError) Error() string {
	if err == nil {
		return "<nil>"
	}

	s := fmt.Sprintf("%s:%d %s", err.file, err.line, err.msg)

	if err.cause != nil {
		return s + lineSeparator + err.cause.Error()
	}

	return s
}

func (err *chainedError) String() string {
	return err.Error()
}

func newChainedError(cause error, file string, line int, msg string) *chainedError {
	return &chainedError{
		cause: cause,
		file:  file,
		line:  line,
		msg:   msg,
	}
}

func cherrf(cause error, format string, a ...interface{}) *chainedError {
	return reachf(2, cause, format, a...)
}

func errorf(format string, a ...interface{}) *chainedError {
	return reachf(2, nil, format, a...)
}

func reachf(calldepth int, cause error, format string, a ...interface{}) *chainedError {
	s := fmt.Sprintf(format, a...)
	file, line := fileAndLine(calldepth)
	return newChainedError(cause, file, line, s)
}

func fileAndLine(calldepth int) (string, int) {
	_, file, line, ok := runtime.Caller(calldepth + 1)
	if !ok {
		file = "???"
		line = 0
	}
	return file, line
}
