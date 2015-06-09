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

// The invalid span ID, which is all zeroes.
htrace.INVALID_SPAN_ID = "0000000000000000";

htrace.Span = Backbone.Model.extend({
  // Parse a span sent from htraced.
  // We use more verbose names for some attributes.
  // Missing attributes are treated as zero or empty.  Numerical attributes are
  // forced to be numbers.
  parse: function(response, options) {
    var span = {};
    this.set("spanId", response.s ? response.s : htrace.INVALID_SPAN_ID);
    this.set("traceId", response.i ? response.i : htrace.INVALID_SPAN_ID);
    this.set("processId", response.r ? response.r : "");
    this.set("parents", response.p ? response.p : []);
    this.set("description", response.d ? response.d : "");
    this.set("begin", response.b ? parseInt(response.b, 10) : 0);
    this.set("end", response.e ? parseInt(response.e, 10) : 0);
    return span;
  },

  // Transform a span model back into a JSON string suitable for sending over
  // the wire.
  unparse: function() {
    var obj = { };
    if (!(this.get("spanId") === htrace.INVALID_SPAN_ID)) {
      obj.s = this.get("spanId");
    }
    if (!(this.get("traceId") === htrace.INVALID_SPAN_ID)) {
      obj.i = this.get("traceId");
    }
    if (!(this.get("processId") === "")) {
      obj.r = this.get("processId");
    }
    if (this.get("parents").length > 0) {
      obj.p = this.get("parents");
    }
    if (this.get("description").length > 0) {
      obj.d = this.get("description");
    }
    if (this.get("begin") > 0) {
      obj.b = this.get("begin");
    }
    if (this.get("end") > 0) {
      obj.e = this.get("end");
    }
    return obj;
  }
});
