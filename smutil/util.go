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

package smutil

import "github.com/samuel/go-zookeeper/zk"

// delete the specified path and its children, recursively.
func DeleteRecursive(c *zk.Conn, path string) error {
	children, _, err := c.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			// if the node doesn't even exist, we are done!
			return nil
		}
		return err
	}

	for _, child := range children {
		p := path + "/" + child
		if err := DeleteRecursive(c, p); err != nil {
			return err
		}
	}

	return c.Delete(path, -1)
}
