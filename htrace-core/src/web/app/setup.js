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

var BaseView = Backbone.Marionette.LayoutView.extend({
  "el": "body",
  "regions": {
    "header": "#header",
    "app": "#app"
  }
});

var Router = Backbone.Marionette.AppRouter.extend({
  "routes": {
    "": "init",
    "!/search(/:query)": "search",
    "!/spans/:id": "span"
  },

  "initialize": function() {
    // Collection
    this.spansCollection = new app.Spans();
    this.spansCollection.fetch();
  },

  "init": function() {
    Backbone.history.navigate("!/search", {"trigger": true});
  },

  "search": function(query) {
    var top = new app.SearchView();
    app.root.app.show(top);

    top.controls.show(new app.SearchControlsView({
      "collection": this.spansCollection
    }));
    top.main.show(new Backgrid.Grid({
      "collection": this.spansCollection,
      "columns": [{
        "name": "spanId",
        "label": "ID",
        "cell": "string",
        "editable": false
      }, {
        "name": "description",
        "label": "Description",
        "cell": "string",
        "editable": false
      }],
      "row": Backgrid.Row.extend({
        "events": {
          "click": "details"
        },
        "details": function() {
          Backbone.history.navigate("!/spans/" + this.model.get("spanId"), {"trigger": true});
        }
      })
    }));
    top.pagination.show(new Backgrid.Extension.Paginator({
      collection: this.spansCollection,
    }));
  },

  "span": function(id) {
    var top = new app.DetailsView();
    app.root.app.show(top);
    top.span.show(new app.SpanDetailsView({
      "model": this.spansCollection.findWhere({
        "spanId": id
      })
    }));
  }
});

app.on("start", function(options) {
  app.root = new BaseView();
  app.routes = new Router();

  Backbone.history.start();
});

app.start();
