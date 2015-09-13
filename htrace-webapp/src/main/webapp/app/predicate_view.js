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

htrace.PredicateView = Backbone.View.extend({
  initialize: function(options) {
    this.el = options.el;
    this.index = options.index;
    this.ptype = options.ptype;
    this.searchView = options.searchView;
  },

  events: {
    "click .closeButton": "remove",
  },

  render: function() {
    this.$el.html(_.template($("#predicate-template").html())
        ({ desc: this.ptype.name, id: this.index }))
    if (this.getText() === "") {
      $(this.$el).find(".form-control").val(this.ptype.getDefaultValue());
    }
    console.log(this.toString() + "#render");
    return this;
  },

  // Handle the user removing this predicate.
  remove: function() {
    this.searchView.removePredicateView(this);
    Backbone.View.prototype.remove.apply(this, arguments);
  },

  // Get the text which the user has entered in.
  getText: function() {
    return $(this.$el).find(".form-control").val().trim();
  },

  // Get the predicate expressed by this view.
  // Throw an exception if the predicate can't be parsed.
  getPredicate: function() {
    return new htrace.Predicate({
        ptype: this.ptype,
        val: this.ptype.normalize(this.getText())
    });
  },

  toString: function() {
    return "PredicateView(this.el=" + this.el + ", this.index=" +
        this.index + ", this.ptype='" + this.ptype.name + "')";
  }
});
