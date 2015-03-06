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

// Span model
app.Span = Backbone.Model.extend({
  "defaults": {
    "spanId": null,
    "traceId": null,
    "processId": null,
    "parents": null,
    "description": null,
    "beginTime": 0,
    "stopTime": 0
  },

  shorthand: {
    "s": "spanId",
    "b": "beginTime",
    "e": "stopTime",
    "d": "description",
    "r": "processId",
    "p": "parents",
    "i": "traceId"
  },

  parse: function(response, options) {
    var attrs = {};
    var $this = this;
    $.each(response, function(key, value) {
      attrs[(key in $this.shorthand) ? $this.shorthand[key] : key] = value;
    });
    return attrs;
  },

  duration: function() {
    return this.get('stopTime') - this.get('beginTime');
  }
});

app.Spans = Backbone.PageableCollection.extend({
  model: app.Span,
  mode: "infinite",
  url: "/query",
  state: {
    pageSize: 10,
    lastSpanId: null,
    predicates: []
  },
  queryParams: {
    totalPages: null,
    totalRecords: null,
    firstPage: null,
    lastPage: null,
    currentPage: null,
    pageSize: null,
    sortKey: null,
    order: null,
    directions: null,

    /**
     * Query parameter for htraced.
     */
    query: function() {
      var predicates = this.state.predicates.slice(0);
      var lastSpanId = this.state.lastSpanId;

      /**
       * Use last pulled span ID to paginate.
       * The htraced API works such that order is defined by the first predicate.
       * Adding a predicate to the end of the predicates list won't change the order.
       * Providing the predicate on spanid will filter all previous spanids.
       */
      if (lastSpanId) {
        predicates.push({
          "op": "gt",
          "field": "spanid",
          "val": lastSpanId
        });
      }

      return JSON.stringify({
        lim: this.state.pageSize,
        pred: predicates
      });
    }
  },

  initialize: function() {
    this.on("sync", function(collection, response, options) {
      if (response.length == 0) {
        delete this.links[this.state.currentPage];
        this.getPreviousPage();
      }
    }, this);
  },

  parseLinks: function(resp, xhr) {
    if (resp.length >= this.state.pageSize) {
      this.state.lastSpanId = resp[resp.length - 1].s;

      return {
        "next": "/query?query=" + this.queryParams.query.call(this)
      };
    } else {
      this.state.lastSpanId = null;

      return {};
    }
  },

  setPredicates: function(predicates) {
    if (!$.isArray(predicates)) {
      console.error("predicates should be an array");
      return;
    }

    this.state.predicates = predicates;
  }
});
