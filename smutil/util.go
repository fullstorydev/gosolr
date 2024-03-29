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

// ParseNodeName parses a solr node identifier into an IP/hostname and a port.
// Node name is in format of <host>:<port>_solr which <host> could either be an IP or hostname
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

// GetHostname gets the hostname from a Solr node name (ie <host>:<port>_solr).
// If the host part is an ip, then the hostname will be returned by address lookup;
// otherwise it returns the host part assuming it is a hostname.
func GetHostname(solrNode string) string {
	host, _, err := ParseNodeName(solrNode)
	if err != nil {
		return ""
	}
	if net.ParseIP(host) != nil { //then host is an IP address
		ip := host
		if names, err := net.LookupAddr(ip); err != nil {
			return ip // fall back to just using the IP
		} else {
			hostname := names[0]
			// Just return the first part of the hostname
			i := strings.Index(hostname, ".")
			if i > -1 {
				hostname = hostname[:i]
			}
			return hostname
		}
	} else { //then host is a hostname
		return host
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
