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

htrace.showSpanDetails = function(span) {
  var info = {
    spanID: span.get("spanID"),
    begin: htrace.dateToString(span.get("begin"), 10),
    end: htrace.dateToString(span.get("end"), 10),
    duration: ((span.get("end") - span.get("begin")) + " ms")
  };
  var explicitOrder = {
    spanId: 1,
    begin: 2,
    end: 3,
    duration: 4
  };
  keys = ["duration"];
  for(k in span.attributes) {
    if (k == "reifiedChildren") {
      continue;
    }
    if (k == "reifiedParents") {
      continue;
    }
    if (k == "selected") {
      continue;
    }
    if (k == "timeAnnotations") {
      // For timeline annotations, make the times into top-level keys.
      var timeAnnotations = span.get("timeAnnotations");
      for (var i = 0; i < timeAnnotations.length; i++) {
        var key = htrace.dateToString(timeAnnotations[i].t);
        keys.push(key);
        info[key] = timeAnnotations[i].m;
        explicitOrder[key] = 200;
      }
      continue;
    }
    if (k == "infoAnnotations") {
      // For info annotations, move the keys to the top level.
      // Surround them in brackets to make it clear that they are
      // user-defined.
      var infoAnnotations = span.get("infoAnnotations");
      _.each(infoAnnotations, function(value, key) {
        key = "[" + key + "]";
        keys.push(key);
        info[key] = value;
        explicitOrder[key] = 200;
      });
      continue;
    }
    keys.push(k);
    if (info[k] == null) {
      info[k] = span.get(k);
    }
  }
  // We sort the keys so that the stuff we want at the top appears at the top,
  // and everything else is in alphabetical order.
  keys = keys.sort(function(a, b) {
      var oa = explicitOrder[a] || 100;
      var ob = explicitOrder[b] || 100;
      if (oa < ob) {
        return -1;
      } else if (oa > ob) {
        return 1;
      } else if (a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    });
  var len = keys.length;
  var h = '<table style="table-layout:fixed;width:100%;word-wrap:break-word">';
  for (i = 0; i < len; i++) {
    // Make every other row grey to improve visibility.
    var colorString = ((i%2) == 1) ? "#f1f1f1" : "#ffffff";
    h += _.template($("#table-row-template").html())(
        {bgcolor: colorString, key: keys[i], val: info[keys[i]]});
  }
  h += '</table>';
  htrace.showModal(_.template($("#modal-table-template").html())(
      {title: "Span Details", body: h}));
};

// Widget containing the trace span displayed on the canvas.
htrace.SpanWidget = function(params) {
  this.draw = function() {
    this.drawBackground();
    this.drawTracerId();
    this.drawDescription();
  };

  // Draw the background of this span widget.
  this.drawBackground = function() {
    this.ctx.save();
    if (this.span.get("selected")) {
      this.ctx.fillStyle="#ffccff";
    } else {
      this.ctx.fillStyle="#ffffff";
    }
    this.ctx.fillRect(this.x0, this.y0, this.xSize, this.ySize);
    this.ctx.restore();
  }

  // Draw process ID text.
  this.drawTracerId = function() {
    this.ctx.save();
    this.ctx.fillStyle="#000000";
    this.ctx.font = "14px sans-serif";
    this.ctx.beginPath();
    this.ctx.rect(this.x0, this.y0, this.xB - this.x0, this.ySize);
    this.ctx.clip();
    this.ctx.fillText(this.span.get('tracerId'), this.x0, this.yF - 4);
    this.ctx.restore();
  };

  // Draw the span description
  this.drawDescription = function() {
    // Draw the light blue bar representing time.
    this.ctx.save();
    this.ctx.beginPath();
    this.ctx.rect(this.xD, this.y0, this.xF - this.xD, this.ySize);
    this.ctx.clip();
    this.ctx.strokeStyle="#000000";
    this.ctx.fillStyle="#a7b7ff";
    var beginX = this.timeToPosition(this.span.get('begin'));
    var endX = this.timeToPosition(this.span.get('end'));

    // If the span is completely off the screen, draw a diamond at either the
    // beginning or the end of the bar to indicate whether it's too early or too
    // late to be seen.
    if (endX < this.x0) {
      beginX = this.xD;
      endX = this.xD;
    }
    if (beginX > this.xF) {
      beginX = this.xF;
      endX = this.xF;
    }

    var gapY = 1;
    var epsilon = Math.max(2, Math.floor(this.xSize / 1000));
    if (endX - beginX < epsilon) {
      // The time interval is too narrow to see.  Draw a diamond on the point instead.
      this.ctx.beginPath();
      this.ctx.moveTo(beginX, this.y0 + gapY);
      this.ctx.lineTo(beginX + (Math.floor(this.ySize / 2) - gapY),
          this.y0 + Math.floor(this.ySize / 2));
      this.ctx.lineTo(beginX, this.yF - gapY);
      this.ctx.lineTo(beginX - (Math.floor(this.ySize / 2) - gapY),
          this.y0 + Math.floor(this.ySize / 2));
      this.ctx.closePath();
      this.ctx.fill();
    } else {
      // Draw a bar from the start time to the end time.
//      console.log("beginX=" + beginX + ", endX=" + endX +
//          ", begin=" + this.span.get('begin') + ", end=" + this.span.get('end'));
      this.ctx.fillRect(beginX, this.y0 + gapY, endX - beginX,
          this.ySize - (gapY * 2));

      // Draw a dots showing time points where annotations are.
      var annotations = this.span.get('timeAnnotations');
      var annotationY = this.y0 + gapY;
      var annotationW = 4;
      var annotationH = (this.ySize - (gapY * 2)) / 2;
      this.ctx.fillStyle="#419641";
      for (var i = 0; i < annotations.length; i++) {
        this.ctx.fillRect(this.timeToPosition(annotations[i].t), annotationY,
            annotationW, annotationH);
      }
    }

    // Draw description text
    this.ctx.fillStyle="#000000";
    this.ctx.font = "14px sans-serif";
    this.ctx.fillText(this.span.get('description'),
        this.xD + this.xT,
        this.yF - gapY - 2);

    this.ctx.restore();
  };

  // Convert a time in milliseconds since the epoch to an x position.
  this.timeToPosition = function(time) {
    return this.xD +
      (((time - this.begin) * (this.xF - this.xD)) /
        (this.end - this.begin));
  };

  this.handle = function(e) {
    switch (e.type) {
      case "mouseDown":
        if (!htrace.inBoundingBox(e.x, e.y,
              this.x0, this.xF, this.y0, this.yF)) {
          return true;
        }
        if (e.raw.ctrlKey) {
          // If the control key is pressed, we toggle the current selection.
          // The user can create multiple selections this way.
          if (this.span.get("selected")) {
            this.span.set("selected", false);
          } else {
            this.span.set("selected", true);
          }
        } else {
          var that = this;
          this.manager.searchResultsView.applyToAllSpans(function(span) {
              // Note: we don't want to set the selection state unless we need
              // to.  Setting the state (even to the same thing it already is)
              // triggers a full re-render, if the span is one in the results
              // collection.  A full re-render slows us down and disrupts events
              // like double-clicking.
              if (that.span === span) {
                if (!span.get("selected")) {
                  span.set("selected", true);
                }
              } else if (span.get("selected")) {
                span.set("selected", false);
              }
            });
        }
        return true;
      case "draw":
        this.draw();
        return true;
      case "dblclick":
        if (htrace.inBoundingBox(e.x, e.y,
            this.x0, this.xF, this.y0, this.yF)) {
          htrace.showSpanDetails(this.span);
        }
        return true;
    }
  };

  for (var k in params) {
    this[k]=params[k];
  }
  this.xSize = this.xF - this.x0;
  this.ySize = this.yF - this.y0;
  this.xDB = this.xD - this.xB;
  this.manager.register("draw", this);

  var widget = this;
  if ((this.span.get("reifiedParents") == null) && (this.allowUpButton)) {
    new htrace.TriangleButton({
      ctx: this.ctx,
      manager: this.manager,
      direction: "up",
      x0: this.xB + 2,
      xF: this.xB + (this.xDB / 2) - 2,
      y0: this.y0 + 2,
      yF: this.yF - 2,
      callback: function(e) {
        var depth = (e.raw.ctrlKey) ? 100 : 0;
        $.when(widget.span.reifyParentsRecursive(depth)).done(function(result) {
          console.log("reifyParentsRecursive(" + depth + "): result was '" +
              result + "'");
          if (result != "") {
            alert(result);
          } else {
            widget.manager.searchResultsView.render();
          }
        });
      }
    });
  }
  if ((this.span.get("reifiedChildren") == null) && (this.allowDownButton)) {
    new htrace.TriangleButton({
      ctx: this.ctx,
      manager: this.manager,
      direction: "down",
      x0: this.xB + (this.xDB / 2) + 2,
      xF: this.xD - 2,
      y0: this.y0 + 2,
      yF: this.yF - 2,
      callback: function(e) {
        var depth = (e.raw.ctrlKey) ? 100 : 0;
        $.when(widget.span.reifyChildrenRecursive(depth)).done(function (result) {
          console.log("reifyChildrenRecursive(" + depth + "): result was '" +
              result + "'");
          if (result != "") {
            alert(result);
          } else {
            widget.manager.searchResultsView.render();
          }
        });
      }
    });
  }
  this.manager.register("mouseDown", this);
  this.manager.register("dblclick", this);
  return this;
};
