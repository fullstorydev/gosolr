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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrmonitor"
)

// reference: https://cwiki.apache.org/confluence/display/solr/Collections+API

const (
	// returned by RequestStatus
	CallUnknown   = "unknown"
	CallCompleted = "completed"
	CallFailed    = "failed"
	CallNotFound  = "notfound"
	CallRunning   = "running"
	CallSubmitted = "submitted"
)

// ResponseHeader is an object included in just about every Solr response.
type ResponseHeader struct {
	Status int `json:"status"`
	QTime  int `json:"QTime"` // in milliseconds
}

type ErrorRsp struct {
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

var _ json.Unmarshaler = (*ErrorRsp)(nil)

type defaultUnmarshalErrorRsp ErrorRsp

// Usually Solr returns `error` as a JSON object, but (rarely) it's just a JSON string.
func (e *ErrorRsp) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		// It's a string error.
		e.Code = 0
		return json.Unmarshal(data, &e.Msg)
	} else {
		// It's a struct error.
		return json.Unmarshal(data, (*defaultUnmarshalErrorRsp)(e))
	}
}

func (e *ErrorRsp) Error() string {
	if e == nil {
		return "<nil>"
	}

	return fmt.Sprintf("(%d) %s", e.Code, e.Msg)
}

func (e *ErrorRsp) String() string {
	return e.Error()
}

type GenericRsp struct {
	Error          ErrorRsp       `json:"error"`
	ResponseHeader ResponseHeader `json:"responseHeader"`
}

func (g *GenericRsp) ErrorRsp() *ErrorRsp {
	if g.Error.Code > 0 {
		return &g.Error
	}
	return nil
}

type HasErrorRsp interface {
	ErrorRsp() *ErrorRsp
}

type RequestStatusRsp struct {
	GenericRsp
	Status struct {
		State string `json:"state"`
		Msg   string `json:"msg"`
	} `json:"status"`
	RequestId string `json:"requestid"`
}

// CoreStatusRsp is returned by from /admin/cores?action=STATUS
type CoreStatusRsp struct {
	GenericRsp
	Status map[string]CoreStatus `json:"status"` // keyed by core name
}

type CoreStatus struct {
	// note: not all fields are represented here
	Name      string          `json:"name"`
	StartTime time.Time       `json:"startTime"`
	Uptime    int64           `json:"uptime"`
	Index     CoreIndexStatus `json:"index"`
	DataDir   string          `json:"dataDir"`
}

type CoreIndexStatus struct {
	NumDocs             int64 `json:"numDocs"`
	MaxDocs             int64 `json:"maxDoc"`
	DeletedDocs         int64 `json:"deletedDocs"`
	IndexHeapUsageBytes int64 `json:"indexHeapUsageBytes"`
	SegmentCount        int64 `json:"segmentCount"`
	HasDeletions        bool  `json:"hasDeletions"`
	SizeInBytes         int64 `json:"sizeInBytes"`
}

type SolrClient struct {
	HttpClient  *http.Client
	SolrMonitor *solrmonitor.SolrMonitor
	Logger      smutil.Logger
	sleepFunc   func(duration time.Duration)
}

// AddReplica adds a replica (asynchronously, if requestId is not empty).
func (sc *SolrClient) AddReplica(collection, shard, dstNode, requestId string) error {
	params := url.Values{}
	params.Set("action", "ADDREPLICA")
	params.Set("collection", collection)
	params.Set("shard", shard)

	if dstNode != "" {
		params.Set("node", dstNode)
	}

	if requestId != "" {
		params.Set("async", requestId)
	}

	var rsp GenericRsp
	return sc.doCollectionCall(params, &rsp)
}

// DeleteReplica deletes a replica asynchronously.
func (sc *SolrClient) DeleteReplica(collection, shard, replica, requestId string) error {
	params := url.Values{}
	params.Set("action", "DELETEREPLICA")
	params.Set("collection", collection)
	params.Set("shard", shard)
	params.Set("replica", replica)
	params.Set("onlyIfDown", "false") // false is the default, but nice to be explicit

	// collections API doc does not list this as an allowed param, but it is
	if requestId != "" {
		params.Set("async", requestId)
	}

	var rsp GenericRsp
	return sc.doCollectionCall(params, &rsp)
}

// DeleteShard deletes a shard synchronously.
func (sc *SolrClient) DeleteShard(collection, shard string) error {
	params := url.Values{}
	params.Set("action", "DELETESHARD")
	params.Set("collection", collection)
	params.Set("shard", shard)

	var rsp GenericRsp
	return sc.doCollectionCall(params, &rsp)
}

// RequestStatus queries SolrCloud for the current status of a previously-submitted asynchronous api call, returning
// the current status, or an error if Solr could not be contacted.  Note that Solr currently does not provide any
// additional information if the state is "failed" (hence our anemic return values).
func (sc *SolrClient) RequestStatus(requestId string) (string, error) {
	params := url.Values{}
	params.Set("action", "REQUESTSTATUS")
	params.Set("requestid", requestId)

	var rsp RequestStatusRsp
	if err := sc.doCollectionCall(params, &rsp); err != nil {
		return "unknown", err
	}

	return rsp.Status.State, nil
}

// SplitShard splits a shard asynchronously.
func (sc *SolrClient) SplitShard(collection, shard, requestId string) error {
	params := url.Values{}
	params.Set("action", "SPLITSHARD")
	params.Set("collection", collection)
	params.Set("shard", shard)
	params.Set("async", requestId)

	var rsp GenericRsp
	return sc.doCollectionCall(params, &rsp)
}

// ParseNodeName parses a solr node identifier into an IP, a port, and a suffix.
func parseNodeName(node string) (string, string, string, error) {
	parts := strings.SplitN(node, "_", 2)
	if len(parts) != 2 {
		return "", "", "", errors.New("malformed: no underscore present")
	}

	if ip, port, err := net.SplitHostPort(parts[0]); err != nil {
		return "", "", "", fmt.Errorf("%q is not a valid socket", parts[0])
	} else {
		return ip, port, parts[1], nil
	}
}

// call Solr's collections API.
func (sc *SolrClient) doCollectionCall(params url.Values, rsp HasErrorRsp) error {
	// Pick a random live node
	ln, err := sc.SolrMonitor.GetLiveNodes()
	if err != nil {
		return smutil.Cherrf(err, "failed to get live nodes")
	}
	solrNode := ln[rand.Intn(len(ln))]

	// parse the node name to get IP, port, etc.
	ip, port, root, err := parseNodeName(solrNode)
	if err != nil {
		return smutil.Cherrf(err, "failed to parse node name")
	}

	params.Add("wt", "json")
	urls := fmt.Sprintf("http://%s:%s/%s/admin/collections?%s", ip, port, root, params.Encode())

	if err := sc.httpGetJson(urls, rsp); err != nil {
		return smutil.Cherrf(err, "failed to query %s with params: %s", urls, params)
	}

	return nil
}

// call Solr's core API on a specific node.
func (sc *SolrClient) doCoreCall(solrNode string, params url.Values, rsp HasErrorRsp) error {
	// parse the node name to get IP, port, etc.
	ip, port, root, err := parseNodeName(solrNode)
	if err != nil {
		return smutil.Cherrf(err, "failed to parse node name")
	}

	params.Add("wt", "json")
	urls := fmt.Sprintf("http://%s:%s/%s/admin/cores?%s", ip, port, root, params.Encode())

	if err := sc.httpGetJson(urls, rsp); err != nil {
		return smutil.Cherrf(err, "failed to query %s with params: %s", urls, params)
	}

	return nil
}

const duplicateTaskMsg = "Task with the same requestid already exists."

func (sc *SolrClient) httpGetJson(url string, rsp HasErrorRsp) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return smutil.Cherrf(err, "failed to create request")
	}

	rawRsp, didRetry, err := func() (*http.Response, bool, error) {
		var lastErr error
		for retry := uint(0); retry <= 6; retry++ {
			if retry > 0 {
				dur := time.Duration(1<<(retry-1)) * time.Second
				if sc.sleepFunc == nil {
					time.Sleep(dur)
				} else {
					sc.sleepFunc(dur)
				}
			}
			rawRsp, err := sc.HttpClient.Do(req)
			if err != nil {
				sc.Logger.Warningf("attempt %d: error contacting solr: %s", retry+1, err)
				lastErr = err
				continue
			}
			return rawRsp, retry > 0, nil
		}
		return nil, true, lastErr
	}()
	if err != nil {
		return smutil.Cherrf(err, "failed request")
	}
	body, err := readBody(rawRsp.Body)
	bodyText := strings.TrimSpace(string(body))
	if err != nil {
		return smutil.Cherrf(err, "failed to read body: StatusCode=%d", rawRsp.StatusCode)
	}

	if err := jsonDecode(body, rsp); err != nil {
		// If we can't decode the json, just jam the entire body text into the error.
		return smutil.Cherrf(err, "failed to decode json:\n%s", bodyText)
	}

	if rawRsp.StatusCode < 200 || rawRsp.StatusCode >= 300 {
		// If we got a bad HTTP status code, we should also get an ErrorRsp.
		if err := rsp.ErrorRsp(); err == nil || err.Code != rawRsp.StatusCode {
			return smutil.Cherrf(&ErrorRsp{
				Code: rawRsp.StatusCode,
				Msg:  bodyText,
			}, "solr returned a bad status code, but no matching error.code")
		}
	}

	if err := rsp.ErrorRsp(); err != nil {
		if didRetry && rsp.ErrorRsp().Msg == duplicateTaskMsg {
			// Just treat "duplicate task" as success, since a previous call must have succeeded.
			return nil
		}
		return smutil.Cherrf(err, "solr returned an error response")
	}

	return nil
}

func jsonDecode(body []byte, rsp HasErrorRsp) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	return decoder.Decode(rsp)
}

// Reads all bytes in n r, then closes r.
func readBody(r io.ReadCloser) ([]byte, error) {
	ret, readErr := ioutil.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil {
		return ret, readErr
	} else {
		return ret, closeErr
	}
}
