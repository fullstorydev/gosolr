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
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
)

var idCounter = rand.NewSource(time.Now().UnixNano()).Int63() >> 1
var pid = os.Getpid()

func newSolrRequestId() string {
	cid := atomic.AddInt64(&idCounter, 1)
	idStr := fmt.Sprintf("solrman-%d-%d", pid, cid)

	// Not sure what constitutes an acceptable request ID for Solr Cloud so let's hex encode this whole thing
	return hex.EncodeToString([]byte(idStr))
}

// checkRequestStatus queries SolrCloud for the current status of a previously-submitted asynchronous api call,
// returning whether the command has completed (successfully or not), the error message (if the command failed),
// or an error struct if we failed to query Solr for some reason.
func checkRequestStatus(logger smutil.Logger, cmdName string, requestId string, solrc *SolrClient) (isDone bool, errMsg string, err error) {
	status, err := solrc.RequestStatus(requestId)
	if err != nil {
		return false, "", smutil.Cherrf(err, "failed to issue REQUESTSTATUS command")
	}

	logger.Debugf("REQUESTSTATUS command returned status %q for requestid %q", status, requestId)

	switch status {
	case CallCompleted:
		return true, "", nil
	case CallFailed:
		logger.Warningf("async %s command failed (requestid = %q)", cmdName, requestId)
		return true, cmdName + " command failed", nil
	case CallNotFound:
		logger.Warningf("async %s command not found?! (requestid = %q)", cmdName, requestId)
		return false, "", fmt.Errorf("%s command not found", cmdName)
	case CallRunning, CallSubmitted:
		// async command is not done yet
		return false, "", nil
	default:
		return false, "", smutil.Errorf("unknown collections API status")
	}
}
