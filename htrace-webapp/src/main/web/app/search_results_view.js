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
    var view = this;
    this.listenTo(this.searchResults, 'add remove change reset',
      _.debounce(function()  {
      view.render();
    }, 10));

    // Re-render the canvas when the window size changes.
    // Add a debouncer delay to avoid spamming render requests.
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
      y: this.getCanvasY(e),
      raw: e
    });
    this.draw();
  },

  handleMouseUp: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "mouseUp",
      x: this.getCanvasX(e),
      y: this.getCanvasY(e),
      raw: e
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
      y: this.getCanvasY(e),
      raw: e
    });
    this.draw();
  },

  handleDblclick: function(e) {
    e.preventDefault();
    this.widgetManager.handle({
      type: "dblclick",
      x: this.getCanvasX(e),
      y: this.getCanvasY(e),
      raw: e
    });
    this.draw();
  },

  render: function() {
    console.log("SearchResultsView#render.");
    $(this.el).html(_.template($("#search-results-view-template").html()));
    $('#selectedTime').attr('readonly', 'readonly');
    this.canvas = $("#resultsCanvas");
    this.ctx = this.canvas.get(0).getContext("2d");
    this.setupCoordinates();
    this.setupWidgets();
    this.scaleCanvas();
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
    var ratio = this.computeScaleFactor();
    //console.log("scaleCanvas: cssX=" + cssX + ", cssY=" + cssY + ", ratio=" + ratio);
    $('#searchView').css('height', this.canvasY + "px");
    $('#results').css('width', this.viewX + "px");
    $('#results').css('height', this.canvasY + "px");
    $('#resultsView').css('width', this.viewX + "px");
    $('#resultsView').css('height', this.canvasY + "px");
    $('#resultsDiv').css('width', this.viewX + "px");
    $('#resultsDiv').css('height', this.canvasY + "px");
    $('#resultsCanvas').css('width', this.viewX + "px");
    $('#resultsCanvas').css('height', this.canvasY + "px");
    this.ctx.canvas.width = this.viewX * ratio;
    this.ctx.canvas.height = this.canvasY * ratio;
    this.ctx.scale(ratio, ratio);
  },

  //
  // Set up the screen coordinates.
  //
  //  0              xB         xD                   xS         viewX
  //  +--------------+----------+--------------------+-----------+
  //  |ProcessId     | Buttons  | Span Description   | Scrollbar |
  //  +--------------+----------+--------------------+-----------+
  //
  setupCoordinates: function() {
    this.viewX = this.canvas.parent().innerWidth();
    this.viewY = $(window).innerHeight() - $("#header").innerHeight() - 50;
    this.xB = Math.min(300, Math.floor(this.viewX / 5));
    this.xD = this.xB + Math.min(75, Math.floor(this.viewX / 20));
    var scrollBarWidth = Math.min(50, Math.floor(this.viewX / 10));
    this.xS = this.viewX - scrollBarWidth;
    this.canvasY = this.viewY;
  },

  setupWidgets: function() {
    this.widgetManager = new htrace.WidgetManager({searchResultsView: this});

    // Create a SpanWidget for each span we know about
    var spanWidgetHeight = Math.min(25, Math.floor(this.viewY / 32));
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
    if (this.canvasY < groupY) {
      this.canvasY = groupY;
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
    this.timeCursor.yF = this.canvasY;
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
    this.ctx.fillRect(0, 0, this.viewX, this.canvasY);
    this.ctx.restore();

    // Draw all the widgets.
    this.widgetManager.handle({type: "draw"});
  },

  checkCanvasTooSmall: function() {
    if ((this.viewX < 200) || (this.viewY < 200)) {
      this.ctx.fillStyle="#cccccc";
      this.ctx.strokeStyle="#000000";
      this.ctx.fillRect(0, 0, this.viewX, this.viewY);
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
    $("#resultsCanvas").off("dblclick");
    $("#resultsCanvas").on("dblclick", function(e) {
      view.handleDblclick(e);
    });
    $("#resultsCanvas").off("contextmenu");
    $("#resultsCanvas").on("contextmenu", function(e) {
      return false;
    });
  },

  remove: function() {
    $(window).off("resize");
    $("#resultsCanvas").off("mousedown");
    $("#resultsCanvas").off("mouseup");
    $("#resultsCanvas").off("mouseout");
    $("#resultsCanvas").off("mousemove");
    $("#resultsCanvas").off("dblclick");
    $("#resultsCanvas").off("contextmenu");
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
      this.setTimes({begin: d.valueOf()});
    } else if (type === "end") {
      this.setTimes({end: d.valueOf()});
    } else {
      throw "invalid type for handleBeginOrEndChange: expected begin or end.";
    }
    this.render();
  },

  setTimes: function(params) {
    if (params["begin"]) {
      this.begin = params["begin"];
    }
    if (params["end"]) {
      this.end = params["end"];
    }
    if (this.end < this.begin) {
      var b = this.begin;
      this.begin = this.end;
      this.end = b;
    }
    var delta = this.end - this.begin;
    if (delta < this.MINIMUM_TIME_SPAN) {
      var needed = this.MINIMUM_TIME_SPAN - delta;
      this.begin -= (needed / 2);
      this.end += (needed / 2);
    }
    $("#begin").val(htrace.dateToString(this.begin));
    $("#end").val(htrace.dateToString(this.end));
    // caller should invoke render()
  },

  clearHandler: function() {
    console.log("invoking clearHandler.");
    var toDelete = []
    var noneSelected = true;
    for (var i = 0; i < this.searchResults.length; i++) {
      var resultSelected = false;
      var model = this.searchResults.at(i);
      htrace.treeTraverseDepthFirstPre(model,
        htrace.getReifiedChildren, 0,
          function(node, depth) {
            if (noneSelected) {
              if (node.get("selected")) {
                resultSelected = true;
              }
            }
          });
      htrace.treeTraverseDepthFirstPre(model,
        htrace.getReifiedParents, 0,
          function(node, depth) {
            if (node.get("selected")) {
              resultSelected = true;
            }
          });
      if (resultSelected) {
        if (noneSelected) {
          toDelete = [];
          noneSelected = false;
        }
        toDelete.push(model);
      } else if (noneSelected) {
        toDelete.push(model);
      }
    }
    this.render();
    ids = [];
    for (var i = 0; i < toDelete.length; i++) {
      ids.push(toDelete[i].get("spanId"));
    }
    console.log("clearHandler: removing " + JSON.stringify(ids));
    this.searchResults.remove(toDelete);
  },

  getSelectedSpansOrAllSpans: function() {
    // Get the list of selected spans.
    // If there are no spans selected, we return all spans.
    var ret = [];
    var noneSelected = true;
    this.applyToAllSpans(function(span) {
        if (span.get("selected")) {
          if (noneSelected) {
            ret = [];
            noneSelected = false;
          }
          ret.push(span);
        } else if (noneSelected) {
          ret.push(span);
        }
      });
    return ret;
  },

  zoomHandler: function() {
    var zoomSpans = this.getSelectedSpansOrAllSpans();
    var numResults = zoomSpans.length;
    if (numResults == 0) {
      this.setTimes({begin:0, end:this.MINIMUM_TIME_SPAN});
      this.render();
      return;
    }
    var minStart = 4503599627370496;
    var maxEnd = 0;
    for (var i = 0; i < numResults; i++) {
      var begin = zoomSpans[i].getEarliestBegin();
      if (begin < minStart) {
        minStart = begin;
      }
      var end = zoomSpans[i].getLatestEnd();
      if (end > maxEnd) {
        maxEnd = end;
      }
    }
    this.setTimes({begin: minStart, end: maxEnd});
    this.render();
  },

  // Apply a function to all spans
  applyToAllSpans: function(cb) {
    for (var i = 0; i < this.searchResults.length; i++) {
      htrace.treeTraverseDepthFirstPre(this.searchResults.at(i),
        htrace.getReifiedChildren, 0,
          function(node, depth) {
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
