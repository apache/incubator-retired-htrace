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
App.Span = Backbone.Model.extend({
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

App.Spans = Backbone.Collection.extend({
  model: App.Span,
  url: "/query",

  initialize: function(models, options) {
    this.predicates = [];
    return Backbone.Collection.prototype.initialize.apply(this, arguments);
  },

  fetch: function(options) {
    options = options ? _.clone(options) : {};
    options.data = {
      "query": {
        "lim": 100000
      }
    };

    if (this.predicates.length > 0) {
      options.data.query.pred = this.predicates;
    }

    options.data.query = JSON.stringify(options.data.query);

    return Backbone.Collection.prototype.fetch.apply(this, [options]);
  },

  setPredicates: function(predicates) {
    this.predicates = predicates;
  }
});
