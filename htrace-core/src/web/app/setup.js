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

var Router = Backbone.Router.extend({

  routes: {
    "": "search",
    "spans/:span": "span"
  },

  initialize: function() {
    this.spansCollection = new App.Spans();
    this.spansCollection.fetch();

    this.spanViews = {};

    this.listSpansView = new App.ListSpansView({
      "collection": this.spansCollection
    }).render();
    $("#list *[role='main']").append(this.listSpansView.$el);

    this.searchView = new App.SearchView({
      "collection": this.spansCollection,
      "el": $("#list").find("[role='form']")
    });
  },

  search: function() {
    $("*[role='application']").css('display', 'none');
    $("#list").show();
  },

  span: function(span) {
    var root = $("#span");

    // Cache views to avoid leaks
    if (!(span in this.spanViews)) {
      var model = this.spansCollection.findWhere({
        "spanId": span
      });

      if (!model) {
        urlconf.navigate("/", true);
        return;
      }

      this.spanViews[span] = new App.SpanView({
        "model": model,
        "id": "span-details"
      });
    }

    var view = this.spanViews[span];

    $("*[role='application']").css('display', 'none');

    view.render();
    root.find("*[role='main']").empty();
    root.find("*[role='main']").append(view.$el);

    root.show();
  }
});

window.urlconf = new Router();

$(function() {
  Backbone.history.start();

  $(".datepicker").pickadate();
  $(".timepicker").pickatime();
});
