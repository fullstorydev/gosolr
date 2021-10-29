package solrmonitor

type SolrEventListener interface {
	SolrLiveNodesChanged(livenodes []string)
	SolrCollectionsChanged(collections []string)
	SolrCollectionChanged(name string, collectionState *CollectionState)
}
