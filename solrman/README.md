# solrman

Test

[![Solar Man by RazorsEdge701 on DeviantArt](solrman.jpg)](http://razorsedge701.deviantart.com/art/Solar-Man-52943088)

Automatically balances a Solr cloud cluster.  Attempts to balance collections, total document count, and total disk size
usage across all the nodes in a solr cluster.

## Features

- Optimizes disk use, doc counts, and collection-level balance across all the nodes in a solr cluster.
- "Moves" shards from one node to another.
- Automatically splits shards that get too big.
- Monitors overall cluster state and halts if anything seems wrong.

## Caveats

- *solrman eliminates replicas*! The current version is not configurable for multiple replicas per shard.
  Running solrman against an existing cluster that has replicas will delete your extra replicas.
- Certain paramaters are hard coded, such as how many documents per shard will trigger a split.
  Currenly you must change the code to tune these for your needs.
- Does NOT support solr collection state format v1.  See note below.
- Automatic shard splitting is likely to cause problems prior to Solr 6.1, see note below.

## Supported versions

Basic operation should work with any Solr 5x or 6x.  Last tested with 5.5.2
However, please note that automatic shard splitting should probably be disabled prior to Solr 6.1 due to
[extremely poor overseer operation with shard splits](https://issues.apache.org/jira/browse/SOLR-8744).

## Does NOT support stateformat v1

If you have any collections in stateformat v1, you must first use the Solr collections API to
[migrate to state format v2](https://cwiki.apache.org/confluence/display/solr/Collections+API#CollectionsAPI-MigrateClusterState)
before running solrman against your cluster.
