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


App.ListSpansView = Backbone.View.extend({
  "tagName": "div",

  "initialize": function() {
    _.bindAll(this, "render");
    this.collection.bind('change', this.render);

    this.rendered = false;

    this.listSpansView = new Backgrid.Grid({
      collection: this.collection,
      columns: [{
        name: "spanId",
        label: "ID",
        cell: "string",
        editable: false
      }, {
        name: "description",
        label: "Description",
        cell: "string",
        editable: false
      }],
      row: Backgrid.Row.extend({
        events: {
          "click": "details"
        },
        details: function() {
          urlconf.navigate("/spans/" + this.model.get("spanId"), true);
        }
      })
    });
  },

  "render": function() {
    $(this.listSpansView.$el).detach();

    this.listSpansView.render();

    $(this.$el).append(this.listSpansView.$el);

    return this;
  }
});