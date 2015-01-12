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
 
App.ListSpansView = Backbone.View.extend({
  "tagName": "ul",
  "className": "spans",
  "template": _.template($("#list-span-template").html()),
  "events": {
    "click li": "spanClicked"
  },

  initialize: function() {
    _.bindAll(this, "render");
    this.collection.bind('change', this.render);

    this.rendered = false;
  },

  "render": function() {
    if (this.rendered) {
      $(this.el).empty();
    }

    $(this.el).append(
        this.template({
          "spans": this.collection.toJSON()
        }));

    this.rendered = true;

    return this;
  },

  "spanClicked": function(e) {
    e.preventDefault();
    var spanId = $(e.currentTarget).data("id");
    window.urlconf.navigate("/spans/" + spanId, true);
  }
});

App.SpanView = Backbone.View.extend({
  "tagName": "div",
  "className": "span",
  "template": _.template($("#span-details-template").html()),

  initialize: function() {
    _.bindAll(this, "render");
    this.model.bind('change', this.render);

    this.rendered = false;
  },

  "render": function() {
    var context = {
      "span": this.model.toJSON()
    };
    context["span"]["duration"] = this.model.duration();

    if (this.rendered) {
      $(this.el).empty();
    }

    $(this.el).append(this.template(context));
    this.rendered = true;

    return this;
  }
});
