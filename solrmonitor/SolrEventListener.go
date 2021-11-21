package solrmonitor

/**
The client can register the SolrEventListener to listen to the solr cluster events from the zookeeper.
 */
type SolrEventListener interface {
	// all the live nodes in the solr cluster
	SolrLiveNodesChanged(livenodes []string)

	// all the query aggregator nodes in ths solr cluster
	SolrQueryNodesChanged(querynodes []string)

	// all the collections in the solr cluster
	SolrCollectionsChanged(collections []string)

	// current collection state change
	SolrCollectionStateChanged(name string, collectionState *CollectionState)

	// collection replica state changed
	SolrCollectionReplicaStatesChanged(name string, replicaStates *map[string]*PerReplicaState)
}
