// Copyright 2017 FullStory, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package smstorage

import (
	"bytes"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/zk"
)

func NewZkStorage(conn *zk.Conn, root string, logger smutil.Logger) (*ZkStorage, error) {
	ret := &ZkStorage{conn: conn, root: root, logger: logger}
	if err := ret.init(); err != nil {
		return nil, err
	}
	return ret, nil
}

type ZkStorage struct {
	conn   *zk.Conn
	root   string
	logger smutil.Logger
}

var _ SolrManStorage = &ZkStorage{}

func (s *ZkStorage) init() error {
	for _, path := range []string{s.root, s.inProgressPath(), s.completedPath(), s.evacuatePath()} {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) inProgressPath() string {
	return s.root + "/in_progress_op_map"
}

func (s *ZkStorage) completedPath() string {
	return s.root + "/completed_op_list"
}

func (s *ZkStorage) evacuatePath() string {
	return s.root + "/evacuate_node_list"
}

func (s *ZkStorage) disabledPath() string {
	return s.root + "/disabled"
}

func (s *ZkStorage) disableSplitsPath() string {
	return s.root + "/disable_splits"
}

func (s *ZkStorage) disableTripsPath() string {
	return s.root + "/disable_trips"
}

func (s *ZkStorage) disableMovesPath() string {
	return s.root + "/disable_moves"
}

func (s *ZkStorage) enableStabbingPath() string {
	return s.root + "/enable_stabbing"
}

func (s *ZkStorage) enableQueryAggregatorStabbingPath() string {
	return s.root + "/enable_stabbing_qa"
}

func (s *ZkStorage) AddInProgressOp(op solrmanapi.OpRecord) error {
	path := s.inProgressPath() + "/" + op.Key()
	data := []byte(jsonString(&op))
	_, err := s.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		_, err = s.conn.Set(path, data, -1)
	}
	if err != nil {
		return smutil.Cherrf(err, "could not create save op at %s in ZK", path)
	}
	return nil
}

func (s *ZkStorage) DelInProgressOp(op solrmanapi.OpRecord) error {
	path := s.inProgressPath() + "/" + op.Key()
	err := s.conn.Delete(path, -1)
	if err != nil && err != zk.ErrNoNode {
		return smutil.Cherrf(err, "could not delete op at %s in ZK", path)
	}
	return nil
}

func (s *ZkStorage) GetInProgressOps() ([]solrmanapi.OpRecord, error) {
	path := s.inProgressPath()
	children, _, err := s.conn.Children(path)
	if err == zk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, smutil.Cherrf(err, "could not get children at %s in ZK", path)
	}

	ret := s.fetchOps(path, children)
	sort.Sort(solrmanapi.ByStartedRecently(ret))
	return ret, nil
}

func (s *ZkStorage) AddCompletedOp(op solrmanapi.OpRecord) error {
	seqPath := s.completedPath() + "/" + "completed-"
	data := []byte(jsonString(&op))
	_, err := s.conn.Create(seqPath, data, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return smutil.Cherrf(err, "could not create create completed op at %s in ZK", seqPath)
	}

	// If there are too many completed ops, delete the eldest.
	path := s.completedPath()
	ok, stat, err := s.conn.Exists(path)
	if err != nil || !ok {
		s.logger.Warningf("could not stat %s in ZK: %s", path, err)
		return nil
	}

	if stat.NumChildren > NumStoredCompletedOps {
		children, _, err := s.conn.Children(path)
		if err != nil {
			s.logger.Warningf("could not get children at %s in ZK: %s", path, err)
			return nil
		}

		// Since these nodes are all sequential, just delete the first N.
		sort.Strings(children)
		if len(children) > NumStoredCompletedOps {
			// In the case that the number of children changes
			// between the time we stat the path and actually
			// get the children, we do this check.
			toDelete := children[:len(children)-NumStoredCompletedOps]
			for _, child := range toDelete {
				childPath := path + "/" + child
				err := s.conn.Delete(childPath, -1)
				if err != nil && err != zk.ErrNoNode {
					s.logger.Warningf("could not delete old completed op at %s in ZK: %s", childPath, err)
				} else {
					s.logger.Debugf("deleted %s from ZK", childPath)
				}
			}
		}
	}

	return nil
}

func (s *ZkStorage) GetCompletedOps(count int) ([]solrmanapi.OpRecord, error) {
	path := s.completedPath()
	children, _, err := s.conn.Children(path)
	if err == zk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, smutil.Cherrf(err, "could not get children at %s in ZK", path)
	}

	ret := s.fetchOps(path, children)
	sort.Sort(solrmanapi.ByFinishedRecently(ret))

	// A negative count returns all completed operations.
	if count < 0 {
		return ret, nil
	} else if len(ret) > count {
		ret = ret[:count]
	}
	return ret, nil
}

func (s *ZkStorage) GetEvacuateNodeList() ([]string, error) {
	path := s.evacuatePath()
	children, _, err := s.conn.Children(path)
	if err == zk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, smutil.Cherrf(err, "could not get children at %s in ZK", path)
	}
	sort.Strings(children)
	return children, nil
}

func (s *ZkStorage) IsDisabled() bool {
	path := s.disabledPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return true // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetDisabled(disabled bool) error {
	path := s.disabledPath()
	if disabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) GetDisabledTime() time.Time {
	path := s.disabledPath()
	exists, stat, err := s.conn.Exists(path)
	if err != nil || !exists {
		if s.logger != nil && err != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return time.Time{}
	}
	// the Ctime is in ms since epoch, so convert before returning
	return time.Unix(stat.Ctime/1000, 0)
}

func (s *ZkStorage) IsSplitsDisabled() bool {
	path := s.disableSplitsPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return true // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetSplitsDisabled(disabled bool) error {
	path := s.disableSplitsPath()
	if disabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) AreTripsDisabled() bool {
	path := s.disableTripsPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return true // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetTripsDisabled(disabled bool) error {
	path := s.disableTripsPath()
	if disabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) IsMovesDisabled() bool {
	path := s.disableMovesPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return true // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetMovesDisabled(disabled bool) error {
	path := s.disableMovesPath()
	if disabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) IsStabbingEnabled() bool {
	path := s.enableStabbingPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return false // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetStabbingEnabled(enabled bool) error {
	path := s.enableStabbingPath()
	if enabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) IsQueryAggregatorStabbingEnabled() bool {
	path := s.enableQueryAggregatorStabbingPath()
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		if s.logger != nil {
			s.logger.Errorf("could not check exists at %s in ZK: %s", path, err)
		}
		return false // assume disabled if we have an error
	}
	return exists
}

func (s *ZkStorage) SetQueryAggregatorStabbingEnabled(enabled bool) error {
	path := s.enableQueryAggregatorStabbingPath()
	if enabled {
		_, err := s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return smutil.Cherrf(err, "could not create %s in ZK", path)
		}
	} else {
		err := s.conn.Delete(path, -1)
		if err != nil && err != zk.ErrNoNode {
			return smutil.Cherrf(err, "could not delete %s in ZK", path)
		}
	}
	return nil
}

func (s *ZkStorage) fetchOps(path string, children []string) []solrmanapi.OpRecord {
	ch := make(chan solrmanapi.OpRecord, len(children))
	var wg sync.WaitGroup
	for _, child := range children {
		wg.Add(1)
		go func(child string) {
			defer wg.Done()
			childPath := path + "/" + child
			data, _, err := s.conn.Get(childPath)
			if err == zk.ErrNoNode {
				return // could be a valid race condition, just ignore this child
			} else if err != nil {
				if s.logger != nil {
					s.logger.Errorf("could not get %s in ZK: %s", childPath, err)
				}
				return
			}

			op := solrmanapi.OpRecord{}
			if err := json.NewDecoder(bytes.NewReader(data)).Decode(&op); err != nil {
				if s.logger != nil {
					s.logger.Errorf("In progress operation found at %s in ZK failed to parse: error=%s\ndata=%s", childPath, err, string(data))
				}
				return
			}
			ch <- op
		}(child)
	}
	wg.Wait()
	close(ch)

	ret := make([]solrmanapi.OpRecord, 0, len(children))
	for op := range ch {
		ret = append(ret, op)
	}
	return ret
}

func jsonString(op *solrmanapi.OpRecord) string {
	if op == nil {
		return ""
	}

	b, err := json.MarshalIndent(op, "", "  ")
	if err != nil {
		panic(err.Error())
	}
	return string(b)
}
