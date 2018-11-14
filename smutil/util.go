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

package smutil

import (
        "errors"
        "fmt"
        "net"
        "strings"

        "github.com/samuel/go-zookeeper/zk"
)

// ParseNodeName parses a solr node identifier into an IP, a port, and a suffix.
func ParseNodeName(node string) (string, string, string, error) {
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

// gethostname performs a DNS lookup on ip and returns the first hostname returned.  If the lookup fails, ip is
// returned.
func GetHostname(solrNode string) string {
        ip, _, _, err := ParseNodeName(solrNode)
        if err != nil {
                return ""
        }
        if names, err := net.LookupAddr(ip); err != nil {
                return ip // fall back to just using the IP
        } else {
                // Just return the first part of the hostname
                hostname := names[0]
                i := strings.Index(hostname, ".")
                if i > -1 {
                        hostname = hostname[:i]
                }
                return hostname
        }
}

// delete the specified path and its children, recursively.
func DeleteRecursive(c *zk.Conn, path string) error {
	children, _, err := c.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			// if the node doesn't even exist, we are done!
			return nil
		}
		return err
	}

	for _, child := range children {
		p := path + "/" + child
		if err := DeleteRecursive(c, p); err != nil {
			return err
		}
	}

	return c.Delete(path, -1)
}
