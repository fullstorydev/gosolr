package solrmonitor

type SolrEventListener interface {
	SolrLiveNodesChanged(livenodes []string)
	SolrQueryNodesChanged(querynodes []string)
	SolrCollectionsChanged(collections []string)
	SolrCollectionChanged(name string, collectionState *CollectionState)
}
