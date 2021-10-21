package solrmonitor

func NewMockSolrMonitor(state ClusterState, liveNodes []string) *SolrMonitor {
	collections := map[string]*collection{}
	for k, v := range state {
		collections[k] = &collection{
				collectionState: v,
		}
	}
	return &SolrMonitor{
		collections: collections,
		liveNodes:   liveNodes,
	}
}
