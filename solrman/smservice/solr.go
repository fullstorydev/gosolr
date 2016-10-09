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
		return cherrf(err, "failed to get live nodes")
	}
	solrNode := ln[rand.Intn(len(ln))]

	// parse the node name to get IP, port, etc.
	ip, port, root, err := parseNodeName(solrNode)
	if err != nil {
		return cherrf(err, "failed to parse node name")
	}

	params.Add("wt", "json")
	urls := fmt.Sprintf("http://%s:%s/%s/admin/collections?%s", ip, port, root, params.Encode())

	if err := httpGetJson(urls, rsp, sc.HttpClient); err != nil {
		return cherrf(err, "failed to query %s with params: %s", urls, params)
	}

	return nil
}

// call Solr's core API on a specific node.
func (sc *SolrClient) doCoreCall(solrNode string, params url.Values, rsp HasErrorRsp) error {
	// parse the node name to get IP, port, etc.
	ip, port, root, err := parseNodeName(solrNode)
	if err != nil {
		return cherrf(err, "failed to parse node name")
	}

	params.Add("wt", "json")
	urls := fmt.Sprintf("http://%s:%s/%s/admin/cores?%s", ip, port, root, params.Encode())

	if err := httpGetJson(urls, rsp, sc.HttpClient); err != nil {
		return cherrf(err, "failed to query %s with params: %s", urls, params)
	}

	return nil
}

func httpGetJson(url string, rsp HasErrorRsp, client *http.Client) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return cherrf(err, "failed to create request")
	}

	rawRsp, err := client.Do(req)
	if err != nil {
		return cherrf(err, "failed request")
	}

	defer drainAndClose(rawRsp.Body)
	if err := checkResponse(rawRsp); err != nil {
		return cherrf(err, "failed request")
	}

	decoder := json.NewDecoder(rawRsp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(rsp); err != nil {
		return cherrf(err, "invalid json response")
	}

	if err := rsp.ErrorRsp(); err != nil {
		return cherrf(err, "solr returned an error response")
	}

	return nil
}

// discards any remaining bytes in r, then closes r.
func drainAndClose(r io.ReadCloser) error {
	_, copyErr := io.Copy(ioutil.Discard, r)
	closeErr := r.Close()
	if closeErr != nil {
		return closeErr
	} else {
		return copyErr
	}
}

func checkResponse(rsp *http.Response) error {
	if rsp.StatusCode >= 200 && rsp.StatusCode <= 299 {
		return nil
	}
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		body = []byte("[failed to read body]")
	}
	rsp.Body.Close()

	return &ErrorRsp{
		Code: rsp.StatusCode,
		Msg:  strings.TrimSpace(string(body)),
	}
}
