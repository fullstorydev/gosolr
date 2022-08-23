package solrmonitor

import "github.com/fullstorydev/zk"

func NewMockSolrMonitor(state ClusterState, liveNodes []string) *SolrMonitor {
	collections := map[string]*collection{}
	for k, v := range state {
		collections[k] = &collection{
			collectionState: v,
		}
	}
	return &SolrMonitor{
		zkCli:       newMockZkClient(),
		collections: collections,
		liveNodes:   liveNodes,
	}
}

func newMockZkClient() ZkCli {
	return mockZkClient{}
}

type mockZkClient struct {
}

func (m mockZkClient) Children(path string) ([]string, *zk.Stat, error) {
	panic("Not implemented")
}

func (m mockZkClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	panic("Not implemented")
}

func (m mockZkClient) Delete(path string, version int32) error {
	panic("Not implemented")
}

func (m mockZkClient) Exists(path string) (bool, *zk.Stat, error) {
	panic("Not implemented")
}

func (m mockZkClient) SessionID() int64 {
	panic("Not implemented")
}

func (m mockZkClient) Set(path string, contents []byte, version int32) (*zk.Stat, error) {
	panic("Not implemented")
}

func (m mockZkClient) Sync(path string) (string, error) {
	panic("Not implemented")
}

func (m mockZkClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	panic("Not implemented")
}

func (m mockZkClient) Get(path string) ([]byte, *zk.Stat, error) {
	panic("Not implemented")
}

func (m mockZkClient) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	panic("Not implemented")
}

func (m mockZkClient) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	panic("Not implemented")
}

func (m mockZkClient) State() zk.State {
	return zk.StateHasSession
}

func (m mockZkClient) Close() {
}
