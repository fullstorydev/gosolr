package smutil

import (
	"fmt"
	"testing"
)

type parseNodeNameResult struct {
	ip   string
	port string
	err  error
}

func TestParseNodeName(t *testing.T) {
	t.Parallel()
	cases := map[string]*parseNodeNameResult{
		"10.240.78.255:8983_solr": {ip: "10.240.78.255", port: "8983", err: nil},
		"192.168.1.1:10101":       {ip: "", port: "", err: fmt.Errorf("malformed solr node identifier: no underscore present in 192.168.1.1:10101")},     // no suffix
		"192.168.1.1:10101solr":   {ip: "", port: "", err: fmt.Errorf("malformed solr node identifier: no underscore present in 192.168.1.1:10101solr")}, // malformed suffix
		"192.168.1.1:foo_solr":    {ip: "", port: "", err: fmt.Errorf("foo is not a valid port")},                                                        // invalid port
		"192.168.1.1_solr":        {ip: "", port: "", err: fmt.Errorf("\"192.168.1.1\" is not a valid socket")},                                          // no port
		"192.168.1.1:_solr":       {ip: "", port: "", err: fmt.Errorf(" is not a valid port")},                                                           // no port
		"solr1:8983_solr":         {ip: "solr1", port: "8983", err: nil},                                                                                 // malformed ip
		":8983_solr":              {ip: "", port: "8983", err: nil},
	}

	for input, expected := range cases {
		ip, port, err := ParseNodeName(input)
		if err == nil && expected.err != nil {
			t.Errorf("error failed for input %q, expected %q got nil", input, expected.err)
		} else if err != nil && expected.err == nil {
			t.Errorf("error failed for input %q, expected nil got %q", input, err)
		} else if err != nil && expected.err != nil && err.Error() != expected.err.Error() {
			t.Errorf("error failed for input %q; expected %q got %q", input, expected.err, err)
		}

		if ip != expected.ip {
			t.Errorf("ip failed for input %q; expected %s got %s", input, expected.ip, ip)
		}
		if port != expected.port {
			t.Errorf("port failed for input %q; expected %s got %s", input, expected.port, port)
		}
	}
}
