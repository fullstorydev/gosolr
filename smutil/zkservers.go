package smutil

import (
	"strings"
)

// Parses a flag of the form `host1:2181,host2:2181/solr` into a ZK server list and solr ZK path.
// Mirrors Solr's interpretation of -DzkHost
func ParseZkServersFlag(zkServers string) (zkHosts []string, solrZkPath string, err error) {
	// TODO(scottb): move -zkServers flag processing to a shared location
	pos := strings.Index(zkServers, "/")
	if pos < 0 {
		return nil, "", Errorf("-zkServers must be of the form host1:2181,host2:2181/solr; missing '/'")
	}
	hosts := zkServers[:pos]
	zkHosts = strings.Split(hosts, ",")
	solrZkPath = strings.TrimSuffix(zkServers[pos:], "/")
	return zkHosts, solrZkPath, nil
}
