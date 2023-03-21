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
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/fullstorydev/zk"
)

// ParseNodeName parses a solr node identifier into an IP/hostname, a port, and a suffix.
func ParseNodeName(node string) (string, string, error) {
	suffixIndex := strings.LastIndex(node, "_")
	if suffixIndex == -1 {
		return "", "", fmt.Errorf("malformed solr node identifier: no underscore present in %s", node)
	}

	hostAndPort := node[:suffixIndex]

	if host, port, err := net.SplitHostPort(hostAndPort); err != nil {
		return "", "", fmt.Errorf("%q is not a valid socket", hostAndPort)
	} else {
		_, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return "", "", fmt.Errorf("%s is not a valid port", port)
		}
		return host, port, nil
	}
}

// GetHostname performs an address lookup on the ip portion of e.g. `127.0.0.1:8983_solr` and returns the first hostname returned.
// If the lookup fails, then ip is returned.
func GetHostname(solrNode string) string {
	ip, _, err := ParseNodeName(solrNode)
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

// ResolveNode performs a DNS resolve on the hostname and return an IP on ip and returns the first hostname returned.
// If the lookup fails, hostName is returned.
func ResolveNode(hostName string) string {
	ip, err := net.ResolveIPAddr("ip4", hostName)
	if err != nil {
		return hostName
	}
	return ip.String()
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
