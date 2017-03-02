package smservice

import (
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
)

type Audit interface {
	BeforeOp(solrmanapi.OpRecord, solrmonitor.CollectionState)
	SuccessOp(solrmanapi.OpRecord, solrmonitor.CollectionState)
	FailedOp(solrmanapi.OpRecord, solrmonitor.CollectionState)
}
