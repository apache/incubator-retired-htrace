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

// A pair of span trees: one going up, and the other going down.
// This represents a single search result.
htrace.SearchResult = Backbone.Model.extend({
  initialize: function(options) {
    this.set("childrenRoot", {
      root: options.span,
      contents: null,
    });
    this.set("childrenRoot", {
      root: options.span,
      contents: null,
    });

    this.set("parentsRoot", options.span);
  },

  getBegin: function() {
    var begin = this.get("span").get("begin");
    var children = this.get("children");
    for (var childIdx = 0; childIdx < children.length; childIdx++) {
      begin = Math.min(begin, children[childIdx].getBegin());
    }
    return begin;
  },

  getEnd: function() {
    var end = this.get("span").get("end");
    var children = this.get("children");
    for (var childIdx = 0; childIdx < children.length; childIdx++) {
      end = Math.max(end, children[childIdx].getEnd());
    }
    return end;
  }
});
