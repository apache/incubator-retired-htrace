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
 
App.SearchView = Backbone.View.extend({
  "events": {
    "click a": "search",
    "click button": "search"
  },

  "search": function() {
    var description = $(this.el).find("input[type='search']").val();
    var begintime = $(this.el).find("#begintime").val();
    var stoptime = $(this.el).find("#stoptime").val();
    var duration = $(this.el).find("#duration").val();

    var newSpans = spans.clone();

    if (begintime) {
      newSpans = newSpans.filter(function(span) {
        return span.get("beginTime") > parseInt(begintime);
      });
    }

    if (stoptime) {
      newSpans = newSpans.filter(function(span) {
        return span.get("stopTime") < parseInt(stoptime);
      });
    }

    if (duration) {
      newSpans = newSpans.filter(function(span) {
        return span.duration() > parseInt(duration);
      });
    }

    newSpans = newSpans.filter(function(span) {
      return span.get("description").toLowerCase().indexOf(description) != -1;
    });

    // TODO: this.collection.fetch
    this.collection.reset(newSpans);

    this.collection.trigger('change');

    return false;
  }
});
