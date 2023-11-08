package solrmonitor

/*
The client can register the SolrEventListener to listen to the solr cluster state events from the zookeeper.

On startup, the events are fired in this order:
 1. SolrLiveNodesChanged fires for all the solr live nodes (node which keeps indexes). It keeps watch on zk node /solr/live_nodes 's children
 2. SolrCollectionsChanged fires for all collections that exist. It keeps watch on zk node /solr/collections 's children.
    2.1 SolrCollectionStateChanged fires for each collection; keeps watch on zk node /solr/collections/collname/state.json
    2.2 SolrCollectionReplicaStatesChanged fires if Per replica state(PRS) is enable for collection.
    If PRS enable then it keeps watch on zk node /solr/collections/collname/state.json 's children. Those children represents state of each replica of collection
    (reference https://issues.apache.org/jira/browse/SOLR-15052, https://docs.google.com/document/d/1xdxpzUNmTZbk0vTMZqfen9R3ArdHokLITdiISBxCFUg/edit )
 3. SolrClusterPropsChanged fires for the single clusterprops file

After initialization it delivers events for following condition

 1. SolrLiveNodesChanged if solr live nodes changes

 2. SolrQueryNodesChanged if solr query node changes

 3. SolrCollectionsChanged if number of collections changes in solr cluster

 4. SolrCollectionStateChanged if collection's number of shards changes, or replica moves, or replica splits.

 5. SolrCollectionReplicaStatesChanged if collection's replica goes up/down, or becomes leader

 6. SolrClusterPropsChanged if the cluster props are modified

 4. If collection get deleted - for non PRS
    4.1 SolrCollectionStateChanged fires for collection's state.json, which get updated for each shard/replica
    4.2	SolrCollectionStateChanged fires with all collections name except deleted collection.

 5. If collection get deleted - for PRS
    5.1 following events happen
    5.1.1 SolrCollectionStateChanged fires for collection's base state.json
    5.1.2 SolrCollectionReplicaStatesChanged fires for all replicas/shards in collection
    5.2 SolrCollectionsChanged fires with all collections name except deleted collection.
*/
type SolrEventListener interface {
	// all the live nodes in the solr cluster
	SolrLiveNodesChanged(livenodes []string)

	// all the collections in the solr cluster
	SolrCollectionsChanged(collections []string)

	// current collection state change
	SolrCollectionStateChanged(name string, collectionState *CollectionState)

	// collection replica state changed
	SolrCollectionReplicaStatesChanged(name string, replicaStates map[string]*PerReplicaState)

	// cluster props changed
	SolrClusterPropsChanged(clusterprops map[string]string)
}
