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

// Convert an array of htrace.Span models into a comma-separated string.
htrace.spanModelsToString = function(spans) {
  var ret = "";
  var prefix = "";
  for (var i = 0; i < spans.length; i++) {
    ret += prefix + JSON.stringify(spans[i].unparse());
    prefix = ", ";
  }
  return ret;
};

// Convert an array of return results from ajax calls into an array of
// htrace.Span models.
htrace.parseMultiSpanAjaxQueryResults = function(ajaxCalls) {
  var parsedSpans = [];
  for (var i = 0; i < ajaxCalls.length; i++) {
    var text = ajaxCalls[i][0];
    var result = ajaxCalls[i][1];
    if (ajaxCalls[i]["status"] != "200") {
      throw "ajax error: " + ajaxCalls[i].statusText;
    }
    var parsedSpan = new htrace.Span({});
    try {
      parsedSpan.parse(ajaxCalls[i].responseJSON, {});
    } catch (e) {
      throw "span parse error: " + e;
    }
    parsedSpans.push(parsedSpan);
  }
  return parsedSpans;
};

htrace.sortSpansByBeginTime = function(spans) {
  return spans.sort(function(a, b) {
      if (a.get("begin") < b.get("begin")) {
        return -1;
      } else if (a.get("begin") > b.get("begin")) {
        return 1;
      } else {
        return 0;
      }
    });
};

htrace.getReifiedParents = function(span) {
  return span.get("reifiedParents") || [];
};

htrace.getReifiedChildren = function(span) {
  return span.get("reifiedChildren") || [];
};

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
    if (response.t) {
      var t = response.t.sort(function(a, b) {
          if (a.t < b.t) {
            return -1;
          } else if (a.t > b.t) {
            return 1;
          } else {
            return 0;
          }
        });
      this.set("timeAnnotations", t);
    } else {
      this.set("timeAnnotations", []);
    }
    this.set("infoAnnotations", response.n ? response.n : {});
    this.set("selected", false);

    // reifiedChildren starts off as null and will be filled in as needed.
    this.set("reifiedChildren", null);

    // If there are parents, reifiedParents starts off as null.  Otherwise, we
    // know it is the empty array.
    this.set("reifiedParents", (this.get("parents").length == 0) ? [] : null);

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
    if (this.get("timeAnnotations").length > 0) {
      obj.t = this.get("timeAnnotations");
    }
    if (_.size(this.get("infoAnnotations")) > 0) {
      obj.n = this.get("infoAnnotations");
    }
    return obj;
  },

  //
  // Although the parent IDs are always present in the 'parents' field of the
  // span, sometimes we need the actual parent span models.  In that case we
  // must "reify" them (make them real).
  //
  // This functionReturns a jquery promise which reifies all the parents of this
  // span and stores them into reifiedParents.  The promise returns the empty
  // string on success, or an error string on failure.
  //
  reifyParents: function() {
    var span = this;
    var numParents = span.get("parents").length;
    var ajaxCalls = [];
    // Set up AJAX queries to reify the parents.
    for (var i = 0; i < numParents; i++) {
      ajaxCalls.push($.ajax({
        url: "span/" + span.get("parents")[i],
        data: {},
        contentType: "application/json; charset=utf-8",
        dataType: "json"
      }));
    }
    var rootDeferred = jQuery.Deferred();
    $.when.apply($, ajaxCalls).then(function() {
      var reifiedParents = [];
      try {
        reifiedParents = htrace.parseMultiSpanAjaxQueryResults(ajaxCalls);
      } catch (e) {
        rootDeferred.resolve("Error reifying parents for " +
            span.get("spanId") + ": " + e);
        return;
      }
      reifiedParents = htrace.sortSpansByBeginTime(reifiedParents);
      // The current span is a child of the reified parents.  There may be other
      // children of those parents, but we are ignoring that here.  By making
      // this non-null, the "expand children" button will not appear for these
      // paren spans.
      for (var j = 0; j < reifiedParents.length; j++) {
        reifiedParents[j].set("reifiedChildren", [span]);
      }
      console.log("Setting reified parents for " + span.get("spanId") +
          " to " + htrace.spanModelsToString (reifiedParents));
      span.set("reifiedParents", reifiedParents);
      rootDeferred.resolve("");
    });
    return rootDeferred.promise();
  },

  //
  // The span itself does not contain its children.  However, the server has an
  // index which can be used to easily find the children of a particular span.
  //
  // This function returns a jquery promise which reifies all the children of
  // this span and stores them into reifiedChildren.  The promise returns the
  // empty string on success, or an error string on failure.
  //
  reifyChildren: function() {
    var rootDeferred = jQuery.Deferred();
    var span = this;
    $.ajax({
        url: "span/" + span.get("spanId") + "/children?lim=50",
        data: {},
        contentType: "application/json; charset=utf-8",
        dataType: "json"
      }).done(function(childIds) {
        var ajaxCalls = [];
        for (var i = 0; i < childIds.length; i++) {
          ajaxCalls.push($.ajax({
            url: "span/" + childIds[i],
            data: {},
            contentType: "application/json; charset=utf-8",
            dataType: "json"
          }));
        };
        $.when.apply($, ajaxCalls).then(function() {
          var reifiedChildren;
          try {
            reifiedChildren = htrace.parseMultiSpanAjaxQueryResults(ajaxCalls);
          } catch (e) {
            reifiedChildren = rootDeferred.resolve("Error reifying children " +
                "for " + span.get("spanId") + ": " + e);
            return;
          }
          reifiedChildren = htrace.sortSpansByBeginTime(reifiedChildren);
          // The current span is a parent of the new child.
          // There may be other parents, but we are ignoring that here.
          // By making this non-null, the "expand parents" button will not
          // appear for these child spans.
          for (var j = 0; j < reifiedChildren.length; j++) {
            reifiedChildren[j].set("reifiedParents", [span]);
          }
          console.log("Setting reified children for " + span.get("spanId") +
              " to " + htrace.spanModelsToString (reifiedChildren));
          span.set("reifiedChildren", reifiedChildren);
          rootDeferred.resolve("");
        });
      }).fail(function(statusData) {
        // Check if the /children query failed.
        rootDeferred.resolve("Error querying children of " +
            span.get("spanId") + ": got " + statusData);
        return;
      });
    return rootDeferred.promise();
  },

  // Get the earliest begin time of this span or any of its reified parents or
  // children.
  getEarliestBegin: function() {
    var earliestBegin = this.get("begin");
    htrace.treeTraverseDepthFirstPre(this, htrace.getReifiedParents, 0,
        function(span, depth) {
          earliestBegin = Math.min(earliestBegin, span.get("begin"));
        });
    htrace.treeTraverseDepthFirstPre(this, htrace.getReifiedChildren, 0,
        function(span, depth) {
          earliestBegin = Math.min(earliestBegin, span.get("begin"));
        });
    return earliestBegin;
  },

  // Get the earliest begin time of this span or any of its reified parents or
  // children.
  getLatestEnd: function() {
    var latestEnd = this.get("end");
    htrace.treeTraverseDepthFirstPre(this, htrace.getReifiedParents, 0,
        function(span, depth) {
          latestEnd = Math.max(latestEnd, span.get("end"));
        });
    htrace.treeTraverseDepthFirstPre(this, htrace.getReifiedChildren, 0,
        function(span, depth) {
          latestEnd = Math.max(latestEnd, span.get("end"));
        });
    return latestEnd;
  },
});
