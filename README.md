# gosolr
FullStory open-source golang tools for Apache Solr

## solrman
A library and application to automatically balance a Solr cluster.

## solrmonitor
A library to monitor Solr cluster state in your own application.

# Quick start
```bash
mkdir solrman
cd solrman
export GOPATH=`pwd`
go get github.com/fullstorydev/gosolr/...
./bin/solrman
```

## Warning

*solrman eliminates replicas*! The current version is not configurable for multiple replicas per shard.
Running solrman against an existing cluster that has replicas will delete your extra replicas.
