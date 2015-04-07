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

app.SearchFieldView = Backbone.View.extend({
  'className': 'search-field',

  'template': _.template($("#search-field-template").html()),

  'events': {
    'change .field': 'showSearchField',
     'click .remove-field': 'destroyField'
  },

  'initialize': function(options) {
    this.options = options;
    this.field = options.field;
  },

  'render': function() {
    this.$el.html(this.template({ field: this.field }));
    this.showSearchField();
    if (this.options.value) this.setValue();
    return this;
  },

  'showSearchField': function() {
    // this.$el.find('.value').hide();
    // this.$el.find('.op').hide();
    // this.$el.find('label').hide();
    this.$el.find('.search-field').hide();
    switch (this.field) {
      case 'begin':
      case 'end':
        this.$el.find('.op').show();
        this.$el.find('.start-end-date-time').show();
        this.$el.find('label.start-end-date-time').text(this.field === 'begin' ? 'Begin' : 'End');
        rome(this.$el.find('#start-end-date-time')[0]);
        break;
      case 'duration':
        this.op = 'ge'
        this.$el.find('.duration').show();
        break;
      case 'description':
        this.op = 'cn'
        this.$el.find('.description').show();
        break;
      default:
        break;
    }
  },

  'destroyField': function(e) {
    this.undelegateEvents();

    $(this.el).removeData().unbind();

    this.remove();
    Backbone.View.prototype.remove.call(this);
    this.options.manager.trigger('removeSearchField', [this.cid]);
  },

  'addPredicate': function() {
    this.options.predicates.push(
      {
        'op': this.op ? this.op : this.$('.op:visible').val(),
        'field': this.field,
        'val': this.getValue()
      }
    );
  },

  'getPredicate': function() {
    return {
      'op': this.op ? this.op : this.$('.op:visible').val(),
      'field': this.field,
      'val': this.getValue()
    };
  },

  'getValue': function() {
    switch (this.field) {
      case 'begin':
      case 'end':
        var now = new moment();
        var datetime = new moment(this.$('input.start-end-date-time:visible').val()).unix();
        return datetime.toString();
      case 'duration':
        return this.$("input.duration:visible").val().toString();
      case 'description':
        return this.$('input.description').val();
      default:
        return '';
    }
  },

  'setValue': function() {
    switch (this.field) {
      case 'begin':
      case 'end':
        this.$('select.op').val(this.options.op);
        this.$('input.start-end-date-time').val(moment.unix(this.options.value).format('YYYY-MM-DD HH:mm'));
      case 'duration':
        this.$("input.duration").val(this.options.value);
      case 'description':
        this.$('input.description').val(this.options.value);
    }
  }
});
