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
    "search": "search",
    "*unknown": "unknown"
  },

  empty: function() {
    console.log("Redirecting to #about.");
    Backbone.history.navigate("about", {"trigger": true, "replace": true});
  },

  about: function() {
    console.log("Visiting #about.");
    serverInfo = new htrace.ServerInfo();
    var router = this;
    serverInfo.fetch({
        "success": function(model, response, options) {
          router.switchView(new htrace.AboutView({model: serverInfo, el: "#app"}));
          router.activateNavBarEntry("about")
        },
        "error": function(model, response, options) {
          window.alert("Failed to fetch htraced server info via GET " +
                       "/server/info: " + JSON.stringify(response));
        }
      });
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
