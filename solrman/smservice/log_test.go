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

package smservice

import (
	"github.com/fullstorydev/gosolr/solrmonitor/smtesting"
)

type testLogger struct {
	logger *smtesting.ZkTestLogger
}

var _ Logger = &testLogger{}

func NewTestLogger(logger *smtesting.ZkTestLogger) *testLogger {
	return &testLogger{logger: logger}
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	l.logger.Printf("DEBUG: "+format, args...)
}

func (l *testLogger) Infof(format string, args ...interface{}) {
	l.logger.Printf("INFO: "+format, args...)
}

func (l *testLogger) Warningf(format string, args ...interface{}) {
	l.logger.Printf("WARN: "+format, args...)
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	l.logger.Printf("ERROR: "+format, args...)
}
