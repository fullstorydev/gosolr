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

func (m mockZkClient) AddPersistentWatch(path string, mode zk.AddWatchMode) (ch zk.EventQueue, err error) {
	panic("implement me")
}

func (m mockZkClient) RemoveAllPersistentWatches(path string) (err error) {
	panic("implement me")
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
