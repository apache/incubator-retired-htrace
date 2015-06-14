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

htrace.SearchResultsView = Backbone.View.extend({
  // The minimum time span we will allow between begin and end.
  MINIMUM_TIME_SPAN: 100,

  begin: 0,

  end: this.MINIMUM_TIME_SPAN,

  initialize: function(options) {
    this.searchResults = options.searchResults;
    this.el = options.el;
    this.listenTo(this.searchResults, 'add remove change reset', this.render);

    // Re-render the canvas when the window size changes.
    // Add a debouncer delay to avoid spamming render requests.
    var view = this;
    $(window).on("resize", _.debounce(function()  {
      view.render();
    }, 250));
  },

  // Get the canvas X coordinate of a mouse click from the absolute event
  // coordinate.
  getCanvasX: function(e) {
    return e.pageX - $("#resultsCanvas").offset().left;
  },

  // Get the canvas Y coordinate of a mouse click from the absolute event
  // coordinate.
  getCanvasY: function(e) {
    return e.pageY - $("#resultsCanvas").offset().top;
  },

  handleMouseDown: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "mouseDown",
      x: this.getCanvasX(e),
      y: this.getCanvasY(e)
    });
    this.draw();
  },

  handleMouseUp: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "mouseUp",
      x: this.getCanvasX(e),
      y: this.getCanvasY(e)
    });
    this.draw();
  },

  handleMouseOut: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "mouseOut"
    });
    this.draw();
  },

  handleMouseMove: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "mouseMove",
      x: this.getCanvasX(e),
      y: this.getCanvasY(e)
    });
    this.draw();
  },

  render: function() {
    console.log("SearchResultsView#render.");
    $(this.el).html(_.template($("#search-results-view-template").html()));
    $('#selectedTime').attr('readonly', 'readonly');
    this.canvas = $("#resultsCanvas");
    this.ctx = this.canvas.get(0).getContext("2d");
    this.scaleCanvas();
    this.setupCoordinates();
    this.setupWidgets();
    this.draw();
    this.attachEvents();
    return this;
  },

  /*
   * Compute the ratio to use between the size of the canvas (i.e.
   * canvas.ctx.width, canvas.ctx.height) and the size in "HTML5 pixels." Note
   * that 'HTML5 pixels" don't actually correspond to screen pixels.  A line 1
   * "HTML5 pixel"  wide actually takes up multiple scren pixels, etc.
   *
   * TODO: fix this to be sharper
   */
  computeScaleFactor: function() {
    var backingStoreRatio = this.ctx.backingStorePixelRatio ||
          this.ctx.mozBackingStorePixelRatio ||
          this.ctx.msBackingStorePixelRatio ||
          this.ctx.webkitBackingStorePixelRatio ||
          this.ctx.oBackingStorePixelRatio ||
          this.ctx.backingStorePixelRatio || 1;
    return (window.devicePixelRatio || 1) / backingStoreRatio;
  },

  // Sets up the canvas size and scaling.
  scaleCanvas: function() {
    var cssX = this.canvas.parent().innerWidth();
    var cssY = $(window).innerHeight() - $("#header").innerHeight() - 50;
    var ratio = this.computeScaleFactor();
    console.log("scaleCanvas: cssX=" + cssX + ", cssY=" + cssY + ", ratio=" + ratio);
    this.maxX = cssX;
    this.maxY = cssY;
    $('#searchView').css('height', cssY + "px");
    $('#results').css('width', cssX + "px");
    $('#results').css('height', cssY + "px");
    $('#resultsView').css('width', cssX + "px");
    $('#resultsView').css('height', cssY + "px");
    $('#resultsDiv').css('width', cssX + "px");
    $('#resultsDiv').css('height', cssY + "px");
    $('#resultsCanvas').css('width', cssX + "px");
    $('#resultsCanvas').css('height', cssY + "px");
    this.ctx.canvas.width = cssX * ratio;
    this.ctx.canvas.height = cssY * ratio;
    this.ctx.scale(ratio, ratio);
  },

  //
  // Set up the screen coordinates.
  //
  //  0              xB         xD                   xS         maxX
  //  +--------------+----------+--------------------+-----------+
  //  |ProcessId     | Buttons  | Span Description   | Scrollbar |
  //  +--------------+----------+--------------------+-----------+
  //
  setupCoordinates: function() {
    this.xB = Math.min(300, Math.floor(this.maxX / 5));
    this.xD = this.xB + Math.min(75, Math.floor(this.maxX / 20));
    var scrollBarWidth = Math.min(50, Math.floor(this.maxX / 10));
    this.xS = this.maxX - scrollBarWidth;
  },

  setupWidgets: function() {
    this.widgetManager = new htrace.WidgetManager({searchResultsView: this});

    // Create a SpanWidget for each span we know about
    var spanWidgetHeight = Math.min(25, Math.floor(this.maxY / 32));
    var numResults = this.searchResults.size();
    var groupY = 0;
    for (var i = 0; i < numResults; i++) {
      var widget = new htrace.SpanGroupWidget({
        manager: this.widgetManager,
        ctx: this.ctx,
        span: this.searchResults.at(i),
        x0: 0,
        xB: this.xB,
        xD: this.xD,
        xF: this.xS,
        y0: groupY,
        begin: this.begin,
        end: this.end,
        spanWidgetHeight: spanWidgetHeight
      });
      groupY = widget.yF;
    }

    // Create the time cursor widget.
    var selectedTime = this.begin;
    if (this.timeCursor != null) {
      selectedTime = this.timeCursor.selectedTime;
    }
    this.timeCursor = new htrace.TimeCursor({
      manager: this.widgetManager,
      selectedTime: selectedTime,
      el: "#selectedTime"
    });
    this.timeCursor.ctx = this.ctx;
    this.timeCursor.x0 = this.xD;
    this.timeCursor.xF = this.xS;
    this.timeCursor.y0 = 0;
    this.timeCursor.yF = this.maxY;
    this.timeCursor.begin = this.begin;
    this.timeCursor.end = this.end;
  },

  draw: function() {
    if (this.checkCanvasTooSmall()) {
      return;
    }

    // Set the background to white.
    this.ctx.save();
    this.ctx.fillStyle="#ffffff";
    this.ctx.strokeStyle="#000000";
    this.ctx.fillRect(0, 0, this.maxX, this.maxY);
    this.ctx.restore();

    // Draw all the widgets.
    this.widgetManager.handle({type: "draw"});
  },

  checkCanvasTooSmall: function() {
    if ((this.maxX < 200) || (this.maxY < 200)) {
      this.ctx.fillStyle="#cccccc";
      this.ctx.strokeStyle="#000000";
      this.ctx.fillRect(0, 0, this.maxX, this.maxY);
      this.ctx.font = "24px serif";
      this.ctx.fillStyle="#000000";
      this.ctx.fillText("Canvas too small!", 0, 24);
      return true;
    }
    return false;
  },

  attachEvents: function() {
    // Use jquery to capture mouse events on the canvas.
    // For some reason using backbone doesn't work for getting these events.
    var view = this;
    $("#resultsCanvas").off("mousedown");
    $("#resultsCanvas").on("mousedown", function(e) {
      view.handleMouseDown(e);
    });
    $("#resultsCanvas").off("mouseup");
    $("#resultsCanvas").on("mouseup", function(e) {
      view.handleMouseUp(e);
    });
    $("#resultsCanvas").off("mouseout");
    $("#resultsCanvas").on("mouseout", function(e) {
      view.handleMouseOut(e);
    });
    $("#resultsCanvas").off("mousemove");
    $("#resultsCanvas").on("mousemove", function(e) {
      view.handleMouseMove(e);
    });
  },

  remove: function() {
    $(window).off("resize");
    $("#resultsCanvas").off("mousedown");
    $("#resultsCanvas").off("mouseup");
    $("#resultsCanvas").off("mousemove");
    Backbone.View.prototype.remove.apply(this, arguments);
  },

  handleBeginOrEndChange: function(e, type) {
    e.preventDefault();
    var text = $(e.target).val().trim();
    var d = null;
    try {
      d = htrace.parseDate(text);
    } catch(err) {
      $("#begin").val(htrace.dateToString(this.begin));
      $("#end").val(htrace.dateToString(this.end));
      htrace.showModalWarning("Timeline " + type + " Format Error",
        "Please enter a valid time in the timeline " + type + " field.<p/>" +
        err);
      return null;
    }
    if (type === "begin") {
      this.setBegin(d.valueOf());
    } else if (type === "end") {
      this.setEnd(d.valueOf());
    } else {
      throw "invalid type for handleBeginOrEndChange: expected begin or end.";
    }
  },

  setBegin: function(val) {
    if (this.end < val + this.MINIMUM_TIME_SPAN) {
      this.begin = val;
      this.end = val + this.MINIMUM_TIME_SPAN;
      console.log("SearchResultsView#setBegin(begin=" + this.begin +
            ", end=" + this.end + ")");
      $("#begin").val(htrace.dateToString(this.begin));
      $("#end").val(htrace.dateToString(this.end));
    } else {
      this.begin = val;
      console.log("SearchResultsView#setBegin(begin=" + this.begin + ")");
      $("#begin").val(htrace.dateToString(this.begin));
    }
    this.render();
  },

  setEnd: function(val) {
    if (this.begin + this.MINIMUM_TIME_SPAN > val) {
      this.begin = val;
      this.end = this.begin + this.MINIMUM_TIME_SPAN;
      console.log("SearchResultsView#setEnd(begin=" + this.begin +
            ", end=" + this.end + ")");
      $("#begin").val(htrace.dateToString(this.begin));
      $("#end").val(htrace.dateToString(this.end));
    } else {
      this.end = val;
      console.log("SearchResultsView#setEnd(end=" + this.end + ")");
      $("#end").val(htrace.dateToString(this.end));
    }
    this.render();
  },

  zoomFitAll: function() {
    var numResults = this.searchResults.size();
    if (numResults == 0) {
      this.setBegin(0);
      this.setEnd(this.MINIMUM_TIME_SPAN);
      return;
    }
    var minStart = 4503599627370496;
    var maxEnd = 0;
    for (var i = 0; i < numResults; i++) {
      var span = this.searchResults.at(i);
      var begin = span.getEarliestBegin();
      if (begin < minStart) {
        minStart = begin;
      }
      var end = span.getLatestEnd();
      if (end > minStart) {
        maxEnd = end;
      }
    }
    this.setBegin(minStart);
    this.setEnd(maxEnd);
  },

  // Apply a function to all spans
  applyToAllSpans: function(cb) {
    for (var i = 0; i < this.searchResults.length; i++) {
      htrace.treeTraverseDepthFirstPre(this.searchResults.at(i),
        htrace.getReifiedChildren, 0,
          function(node, depth) {
            console.log("node = " + node + ", node.constructor.name = " + node.constructor.name);
            cb(node);
          });
      htrace.treeTraverseDepthFirstPre(this.searchResults.at(i),
        htrace.getReifiedParents, 0,
          function(node, depth) {
            if (depth > 0) {
              cb(node);
            }
          });
    }
  }
});
