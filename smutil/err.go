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

package smutil

import (
	"fmt"
	"net"
	"net/url"
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

type CausalError interface {
	error
	Cause() error
}

var _ CausalError = &chainedError{}

// Returns the cause of this error, which may be nil
func (err *chainedError) Cause() error {
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

func Cherrf(cause error, format string, a ...interface{}) CausalError {
	return Reachf(2, cause, format, a...)
}

func Errorf(format string, a ...interface{}) CausalError {
	return Reachf(2, nil, format, a...)
}

func Reachf(calldepth int, cause error, format string, a ...interface{}) CausalError {
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

// Root returns the "root" of an error.  Specifically, if err is a CausalError, Root recusively returns the root cause
// (i.e. the first error in the chain).  Else, if err is a *url.Error or *net.OpError, Root is called recursively on
// the error's Err field.  Otherwise, Root just returns err.
func Root(err error) error {
	if err == nil {
		return err
	}

	// support for a few special built-in error types
	switch v := err.(type) {
	case *url.Error:
		if v.Err != nil {
			return Root(v.Err)
		}
		return v

	case *net.OpError:
		if v.Err != nil {
			return Root(v.Err)
		}
		return v
	}

	if ce, ok := err.(CausalError); ok {
		if cause := ce.Cause(); cause != nil {
			return Root(cause)
		}
	}

	return err
}
