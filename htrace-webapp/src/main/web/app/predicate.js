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

htrace.checkStringIsPositiveWholeNumber = function(val) {
  if (!val.match(/^[0-9]([0-9]*)$/)) {
    if (!val.match(/[^\s]/)) {
      throw "You entered an empty string into a numeric field.";
    }
    throw "Non-numeric characters found.";
  }
};

htrace.checkStringIsNotEmpty = function(val) {
  if (!val.match(/[^\s]/)) {
    throw "You entered an empty string into a text field.";
  }
};

// Predicate type
htrace.PType = Backbone.Model.extend({
  initialize: function(options) {
    this.name = options.name;
    this.field = options.field;
    this.op = options.op;
  },

  // Try to normalize a value of this type into something htraced can accept.
  // Returns a string containing the normalized value on success.  Throws a
  // string explaining the parse error otherwise.
  // Dates are represented by milliseconds since the epoch; span ids don't start
  // with 0x.
  normalize: function(val) {
    switch (this.field) {
    case "begin":
      return htrace.parseDate(val).valueOf().toString();
    case "end":
      return htrace.parseDate(val).valueOf().toString();
    case "description":
      htrace.checkStringIsNotEmpty(val);
      return val;
    case "duration":
      htrace.checkStringIsPositiveWholeNumber(val);
      return val;
    case "spanid":
      return htrace.normalizeSpanId(val);
    case "processid":
      htrace.checkStringIsNotEmpty(val);
      return val;
    default:
      return "Normalization not implemented for field '" + this.field + "'";
    }
  },

  getDefaultValue: function() {
    switch (this.field) {
    case "begin":
      return htrace.dateToString(moment());
    case "end":
      return htrace.dateToString(moment());
    case "description":
      return "";
    case "duration":
      return "0";
    case "spanid":
      return "";
    case "processid":
      return "";
    default:
      return "(unknown)";
    }
  }
});

htrace.parsePType = function(name) {
  switch (name) {
    case "Began after":
      return new htrace.PType({name: name, field:"begin", op:"gt"});
    case "Began at or before":
      return new htrace.PType({name: name, field:"begin", op:"le"});
    case "Ended after":
      return new htrace.PType({name: name, field:"end", op:"gt"});
    case "Ended at or before":
      return new htrace.PType({name: name, field:"end", op:"le"});
    case "Description contains":
      return new htrace.PType({name: name, field:"description", op:"cn"});
    case "Description is exactly":
      return new htrace.PType({name: name, field:"description", op:"eq"});
    case "Duration is longer than":
      return new htrace.PType({name: name, field:"duration", op:"gt"});
    case "Duration is at most":
      return new htrace.PType({name: name, field:"duration", op:"le"});
    case "Span ID is":
      return new htrace.PType({name: name, field:"spanid", op:"eq"});
    case "ProcessId contains":
      return new htrace.PType({name: name, field:"processid", op:"cn"});
    case "ProcessId is exactly":
      return new htrace.PType({name: name, field:"processid", op:"eq"});
    default:
      return null
  }
};

htrace.Predicate = function(options) {
  this.op = options.ptype.op;
  this.field = options.ptype.field;
  this.val = options.val;
  return this;
};
