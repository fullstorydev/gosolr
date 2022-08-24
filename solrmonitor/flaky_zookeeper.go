package solrmonitor

import (
	"errors"
	"github.com/fullstorydev/zk"
	"math/rand"
	"sync/atomic"
)

// TODO: make an integration test using this idea.

// 60% of the time, it works every time
const flakeChance = 0.60

type SexPantherZkCli struct {
	Delegate ZkCli
	Rnd      *rand.Rand
	Flaky    int32
}

var _ ZkCli = &SexPantherZkCli{}

func (s *SexPantherZkCli) SetFlaky(flaky bool) {
	if flaky {
		atomic.StoreInt32(&s.Flaky, 1)
	} else {
		atomic.StoreInt32(&s.Flaky, 0)
	}
}

func (s *SexPantherZkCli) isFlaky() bool {
	return atomic.LoadInt32(&s.Flaky) != 0
}

func (s *SexPantherZkCli) Children(path string) ([]string, *zk.Stat, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return nil, nil, errors.New("flaky error")
	}
	return s.Delegate.Children(path)
}

func (s *SexPantherZkCli) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return "", errors.New("flaky error")
	}
	return s.Delegate.Create(path, data, flags, acl)
}

func (s *SexPantherZkCli) Delete(path string, version int32) error {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return errors.New("flaky error")
	}
	return s.Delegate.Delete(path, version)
}

func (s *SexPantherZkCli) Exists(path string) (bool, *zk.Stat, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return false, nil, errors.New("flaky error")
	}
	return s.Delegate.Exists(path)
}

func (s *SexPantherZkCli) SessionID() int64 {
	return s.Delegate.SessionID()
}

func (s *SexPantherZkCli) Set(path string, contents []byte, version int32) (*zk.Stat, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return nil, errors.New("flaky error")
	}
	return s.Delegate.Set(path, contents, version)
}

func (s *SexPantherZkCli) Sync(path string) (string, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return "", errors.New("flaky error")
	}
	return s.Delegate.Sync(path)
}

func (s *SexPantherZkCli) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return nil, nil, nil, errors.New("flaky error")
	}
	return s.Delegate.ChildrenW(path)
}

func (s *SexPantherZkCli) Get(path string) ([]byte, *zk.Stat, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return nil, nil, errors.New("flaky error")
	}
	return s.Delegate.Get(path)
}

func (s *SexPantherZkCli) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return nil, nil, nil, errors.New("flaky error")
	}
	return s.Delegate.GetW(path)
}

func (s *SexPantherZkCli) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	if s.isFlaky() && s.Rnd.Float32() > flakeChance {
		return false, nil, nil, errors.New("flaky error")
	}
	return s.Delegate.ExistsW(path)
}

func (s *SexPantherZkCli) State() zk.State {
	return s.Delegate.State()
}

func (s *SexPantherZkCli) Close() {
	s.Delegate.Close()
}
