/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

var htrace = htrace || {};

//
// Get the height of a tree-- that is, the number of edges on the longest
// downward path between the root and a leaf
//
htrace.treeHeight = function(node, getDescendants) {
  var height = 0;
  var descendants = getDescendants(node);
  for (var i = 0; i < descendants.length; i++) {
    height = Math.max(height,
        1 + htrace.treeHeight(descendants[i], getDescendants));
  }
  return height;
};

//
// Perform a depth-first, post-order traversal on the tree, invoking the
// callback on every node with the node and depth as the arguments.
//
// Example:
//     5
//    / \
//   3   4
//  / \
// 1   2
//
htrace.treeTraverseDepthFirstPost = function(node, getDescendants, depth, cb) {
  var descendants = getDescendants(node);
  for (var i = 0; i < descendants.length; i++) {
    htrace.treeTraverseDepthFirstPost(descendants[i],
        getDescendants, depth + 1, cb);
  }
  cb(node, depth);
};

//
// Perform a depth-first, pre-order traversal on the tree, invoking the
// callback on every node with the node and depth as the arguments.
//
// Example:
//     1
//    / \
//   2   5
//  / \
// 3   4
//
htrace.treeTraverseDepthFirstPre = function(node, getDescendants, depth, cb) {
  cb(node, depth);
  var descendants = getDescendants(node);
  for (var i = 0; i < descendants.length; i++) {
    htrace.treeTraverseDepthFirstPre(descendants[i],
        getDescendants, depth + 1, cb);
  }
};
