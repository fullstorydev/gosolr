package smutil

import (
	"reflect"
	"testing"
)

func TestParseZkServersFlag(t *testing.T) {
	tcs := []struct {
		input         string
		expectZkHosts []string
		expectZkPath  string
		expectError   bool
	}{
		{"", nil, "", true},
		{"127.0.0.1/solr", []string{"127.0.0.1"}, "/solr", false},
		{"host1:2181,host2:2181/solrroot", []string{"host1:2181", "host2:2181"}, "/solrroot", false},
	}

	for i, tc := range tcs {
		zkHosts, solrZkPath, err := ParseZkServersFlag(tc.input)
		if tc.expectError {
			if err == nil {
				t.Errorf("case %d: Expected error, got none", i)
			}
			if len(zkHosts) != 0 {
				t.Errorf("case %d: Expected empty, got %v", i, zkHosts)
			}
			if solrZkPath != "" {
				t.Errorf("case %d: Expected empty, got %s", i, solrZkPath)
			}
		} else {
			// expect no error
			if err != nil {
				t.Errorf("case %d: Expected no error, got %s", i, err)
			}
			if tc.expectZkPath != solrZkPath {
				t.Errorf("case %d: Expected %s, got %s", i, tc.expectZkPath, solrZkPath)
			}
			if !reflect.DeepEqual(tc.expectZkHosts, zkHosts) {
				t.Errorf("case %d: Expected %v, got %v", i, tc.expectZkHosts, zkHosts)
			}
		}
	}
}
