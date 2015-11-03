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

htrace.HTraceRouter = Backbone.Router.extend({
  "routes": {
    "": "empty",
    "about": "about",
    "serverInfo": "serverInfo",
    "search": "search",
    "*unknown": "unknown"
  },

  empty: function() {
    console.log("Redirecting to #about.");
    Backbone.history.navigate("about", {"trigger": true, "replace": true});
  },

  about: function() {
    console.log("Visiting #about.");
    var router = this;
    router.switchView(new htrace.AboutView({el: "#app"}));
    router.activateNavBarEntry("about")
  },

  serverInfo: function() {
    console.log("Visiting #serverInfo.");
    var version = new htrace.ServerVersion();
    var router = this;
    version.fetch({
      "error": function(model, response, options) {
        window.alert("Failed to fetch htraced server version: " +
                     JSON.stringify(response));
      },
      "success": function(model, response, options) {
        stats = new htrace.ServerStats();
        stats.fetch({
          "error": function(model, response, options) {
            window.alert("Failed to fetch htraced server stats: " +
                         JSON.stringify(response));
          },
          "success": function(model, response, options) {
            router.switchView(new htrace.ServerInfoView({
              model: {
                "version": version,
                "stats": stats
              },
              el: "#app"
            }))
            router.activateNavBarEntry("serverInfo")
          }
        })
      }
    })
  },

  search: function() {
    console.log("Visiting #search.");
    this.switchView(new htrace.SearchView({el : "#app"}));
    htrace.router.activateNavBarEntry("search");
  },

  unknown: function() {
    console.log("Unknown route " + Backbone.history.getFragment() + ".")
  },

  "switchView": function(view) {
    this.view && this.view.close();
    this.view = view;
    this.view.render();
  },

  "activateNavBarEntry": function(id) {
     $(".nav").find(".active").removeClass("active");
     $(".nav").find("#" + id).addClass("active");
  }
});

htrace.router = new htrace.HTraceRouter();
Backbone.history.start();
