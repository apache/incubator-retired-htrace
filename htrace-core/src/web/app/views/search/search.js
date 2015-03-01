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
    "click a.add-field": "addSearchField",
    "click button.search": "search",
  },

  'initialize': function() {
    this.predicates = [];
    this.searchFields = [];
    this.searchFields.push(new App.SearchFieldView({
      predicates: this.predicates,
      manager: this,
      field: 'description'
    }));
    this.on('removeSearchField', this.removeSearchField, this);
  },

  'render': function() {
    this.$('.search-fields').append(this.searchFields[0].render().$el);
    return this;
  },

  'addSearchField': function(e) {
    var target = $(e.target);
    $('button.field').text(target.text());
    var newSearchField = new App.SearchFieldView({
      predicates: this.predicates,
      manager: this,
      field: target.data('field')
    });
    this.$('.search-fields').append(newSearchField.render().$el);
    this.searchFields.push(newSearchField);
  },

  'removeSearchField': function(cid) {
    var removedFieldIndex = _(this.searchFields).indexOf(_(this.searchFields).findWhere({cid: cid}));
    this.searchFields.splice(removedFieldIndex, 1);
  },

  "search": function(e) {
    this.predicates = _(this.searchFields).map(function(field) {
      return field.getPredicate();
    }).filter(function(predicate) {
      return predicate.val;
    });

    this.collection.switchMode("infinite", {
      fetch: false,
      resetState: true
    });
    this.collection.fullCollection.reset();
    this.collection.setPredicates(this.predicates);
    this.collection.fetch();
    return false;
  }
});
