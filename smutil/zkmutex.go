// Copyright 2016 FullStory, Inc.
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

package smutil

import (
	"time"

	"github.com/fullstorydev/zk"
)

// Acquire the ZK mutex at the given path.  This is designed for mutexes which are usually *uncontended*, not
// for general purpose leader election.
//
// On success, returns a close function to release the acquire mutex, otherwise returns an error.
//
// The caller must supply an implementation for `onLostMutex`.  If this ZK session is lost, or the underlying ZK
// mutex node is deleted by an outside party, this implementation will call `onLostMutex`.  The caller must
// release any resources guarded by the mutex.
func AcquireAndMonitorZkMutex(logger Logger, conn *zk.Conn, path string, onLostMutex func()) (func(), error) {
	if err := acquireZkMutex(logger, conn, path); err != nil {
		return func() {}, err
	}
	return monitorZkMutex(logger, conn, path, onLostMutex), nil
}

func acquireZkMutex(logger Logger, conn *zk.Conn, path string) error {
	// Wait for up to a minute trying to acquire.
	timeout := time.After(1 * time.Minute)
	for {
		_, err := conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err == zk.ErrNodeExists {
			// fall through to below and wait to acquire the mutex
		} else if err != nil {
			// unexpected
			return Cherrf(err, "could not create %s in ZK", path)
		} else {
			logger.Debugf("acquired mutex at %s", path)
			return nil
		}

		// Check existence and (most likely) wait to see if the node is deleted.
		exists, stat, eventCh, err := conn.ExistsW(path)
		if err != nil {
			return Cherrf(err, "could not exists %s in ZK", path)
		}

		if exists && stat.EphemeralOwner == 0 {
			// Someone left a non-ephemeral node in the way, this is bad, delete the obstructing node!
			logger.Warningf("a permanent node is obstructing %s in ZK; deleting it", path)
			err := conn.Delete(path, stat.Version)
			if err == zk.ErrNoNode {
				// someone else just deleted it, this is fine
			} else if err == zk.ErrBadVersion {
				// someone else just deleted and recreated it, this is unusual but ok
			} else if err != nil {
				// anything else is not ok
				return Cherrf(err, "could not remove obstructing %s in ZK", path)
			}
			continue // since the obstructing node was deleted, loop and try again
		}

		// Wait to see if the mutex node disappears, then loop and try again.
		logger.Infof("someone else is holding the ZK mutex at %s; waiting...", path)
		select {
		case <-timeout:
			return Cherrf(nil, "failed to acquire solrman mutex, exiting")
		case <-eventCh:
			// loop and try again
		}
	}
}

func monitorZkMutex(logger Logger, conn *zk.Conn, path string, onLostMutex func()) func() {
	shouldExit := make(chan struct{})
	didExit := make(chan struct{})
	go func() {
		defer close(didExit)

		ourVersion := int32(-1)
		defer func() {
			// Clean up our mutex, but only if it's the right version.
			if ourVersion >= 0 {
				if err := conn.Delete(path, ourVersion); err != nil {
					logger.Warningf("failed to delete ZK mutex %s while exiting: %s", path, err)
				} else {
					logger.Debugf("deleting ZK mutex %s while exiting", path)
				}
			}
		}()

		var eventCh <-chan zk.Event
		for {
			if eventCh != nil {
				select {
				case <-shouldExit:
					logger.Debugf("solrman mutex watcher exiting on close")
					return
				case evt := <-eventCh:
					logger.Debugf("solrman mutex event: %+v", evt)
				}
			}

			exists, stat, newEventCh, err := conn.ExistsW(path)
			if err != nil {
				logger.Errorf("could not exists %s which we wrote, defensively exiting: %s", path, err)
				onLostMutex()
				ourVersion = -1
				return
			}
			if !exists || stat.EphemeralOwner != conn.SessionID() {
				logger.Errorf("lost solrman ZK mutex %s, exiting", path)
				onLostMutex()
				ourVersion = -1
				return
			}

			// spin and keep watch
			logger.Debugf("found our ZK mutex at %s, stat=%+v", path, stat)
			eventCh = newEventCh
			ourVersion = stat.Version
		}
	}()

	return func() {
		close(shouldExit)
		<-didExit
	}
}
