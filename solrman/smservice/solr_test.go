package smservice

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/smtestutil"
	"github.com/fullstorydev/gosolr/smutil"
)

const errorStringPayload = `{
  "responseHeader": {
    "status": 0,
    "QTime": 18
  },
  "error": "%s",
  "requestid": "foo"
}`

const errorObjectPayload = `{
  "responseHeader": {
    "status": 0,
    "QTime": 18
  },
  "error": {
    "metadata": [
      "error-class",
      "org.apache.solr.common.SolrException",
      "root-error-class",
      "org.apache.solr.common.SolrException"
    ],
    "msg": "%s",
    "code": 400
  }
}`

func TestJsonDecode(t *testing.T) {
	var rsp GenericRsp
	const errMsg = "error message"
	if err := jsonDecode([]byte(fmt.Sprintf(errorStringPayload, errMsg)), &rsp); err != nil {
		t.Errorf("error: %s", err)
	}
	if rsp.Error.Msg != errMsg {
		t.Errorf("wrong message, expected: %q got %q", errMsg, rsp.Error.Msg)
	}
	if rsp.Error.Code != 0 {
		t.Errorf("wrong code, expected: 0 got %d", rsp.Error.Code)
	}

	if err := jsonDecode([]byte(fmt.Sprintf(errorObjectPayload, errMsg)), &rsp); err != nil {
		t.Errorf("expected no error, got %s", err)
	}
	if rsp.Error.Msg != errMsg {
		t.Errorf("wrong message, expected: %q got %q", errMsg, rsp.Error.Msg)
	}
	if rsp.Error.Code != 400 {
		t.Errorf("wrong code, expected: 400 got %d", rsp.Error.Code)
	}
}

func TestHttpRetries(t *testing.T) {
	goodResponse := func() *http.Response {
		return &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(strings.NewReader(`{}`)),
		}
	}
	const testErrMsg = "test error message"
	badResponse := func() *http.Response {
		return &http.Response{
			StatusCode: 400,
			Body:       ioutil.NopCloser(strings.NewReader(fmt.Sprintf(errorObjectPayload, testErrMsg))),
		}
	}
	errResponse := func() *http.Response {
		return &http.Response{
			StatusCode: 500,
			Body:       ioutil.NopCloser(strings.NewReader(testErrMsg)),
		}
	}
	netErr := net.UnknownNetworkError("test error")
	sleepTimes := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
	}

	tcs := []struct {
		handler     func(*http.Request, int) (*http.Response, error)
		expectCalls int
		expectErr   func(error)
		expectSleep []time.Duration
	}{
		// Basic success, good response.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0:
					return goodResponse(), nil
				default:
					panic("too many calls")
				}
			},
			expectCalls: 1,
			expectSleep: nil,
			expectErr:   nil,
		},
		// Basic success, bad response.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0:
					return badResponse(), nil
				default:
					panic("too many calls")
				}
			},
			expectCalls: 1,
			expectSleep: nil,
			expectErr: func(err error) {
				errRsp := smutil.Root(err).(*ErrorRsp)
				if code := errRsp.Code; code != 400 {
					t.Errorf("expected 400, got %d", code)
				}
				if msg := errRsp.Msg; msg != testErrMsg {
					t.Errorf("expected %q, got %q", testErrMsg, msg)
				}
			},
		},
		// Permanent failure case.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0, 1, 2, 3, 4, 5, 6:
					return nil, netErr
				default:
					panic("too many calls")
				}
			},
			expectCalls: 7,
			expectSleep: sleepTimes,
			expectErr: func(err error) {
				if smutil.Root(err) != netErr {
					t.Errorf("error: %s, expected: %s", err, netErr)
				}
			},
		},
		// Permanent server error case.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0, 1, 2, 3, 4, 5, 6:
					return errResponse(), nil
				default:
					panic("too many calls")
				}
			},
			expectCalls: 7,
			expectSleep: sleepTimes,
			expectErr: func(err error) {
				errRsp := smutil.Root(err).(*ErrorRsp)
				if code := errRsp.Code; code != 500 {
					t.Errorf("expected 500, got %d", code)
				}
				if msg := errRsp.Msg; msg != testErrMsg {
					t.Errorf("expected %q, got %q", testErrMsg, msg)
				}
			},
		},
		// Temporary failure case.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0, 1, 2:
					return nil, netErr
				case 3:
					return goodResponse(), nil
				default:
					panic("too many calls")
				}
			},
			expectCalls: 4,
			expectSleep: sleepTimes[0:3],
			expectErr:   nil,
		},
		// Temporary server error case.
		{
			handler: func(_ *http.Request, call int) (*http.Response, error) {
				switch call {
				case 0, 1, 2:
					return errResponse(), nil
				case 3:
					return goodResponse(), nil
				default:
					panic("too many calls")
				}
			},
			expectCalls: 4,
			expectSleep: sleepTimes[0:3],
			expectErr:   nil,
		},
	}

	logger := smtestutil.NewSmTestLogger(t)
	for i, tc := range tcs {
		t.Logf("case %d", i)
		rt := newRoundTripper(tc.handler)
		var sleepTimes []time.Duration
		sc := &SolrClient{
			HttpClient: &http.Client{
				Transport: &rt,
			},
			SolrMonitor: nil,
			Logger:      logger,
			sleepFunc: func(dur time.Duration) {
				sleepTimes = append(sleepTimes, dur)
			},
		}
		var rsp GenericRsp
		err := sc.httpGetJson("http://localhost:8983/solr/admin/collections", &rsp)
		if rt.calls != tc.expectCalls {
			t.Errorf("expected %d calls, got %d", tc.expectCalls, rt.calls)
		}
		if !reflect.DeepEqual(sleepTimes, tc.expectSleep) {
			t.Errorf("expected %+v sleep, got %+v", tc.expectSleep, sleepTimes)
		}
		if err != nil {
			if tc.expectErr != nil {
				tc.expectErr(err)
			} else {
				t.Errorf("expected no error, got %s", err)
			}
		} else {
			if tc.expectErr != nil {
				t.Errorf("expected error, got no error")
			}
		}
	}
}

type roundTripper struct {
	f     func(*http.Request, int) (*http.Response, error)
	calls int
}

var _ http.RoundTripper = (*roundTripper)(nil)

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	call := rt.calls
	rt.calls++
	return rt.f(req, call)
}

func newRoundTripper(f func(*http.Request, int) (*http.Response, error)) roundTripper {
	return roundTripper{f: f}
}
