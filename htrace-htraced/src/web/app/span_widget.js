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

// Widget containing the trace span displayed on the canvas.
htrace.SpanWidget = function(params) {
  for (var k in params) {
    this[k]=params[k];
  }

  this.selected = false;
  this.widgetManagerFocused = false;
  this.xSize = this.xF - this.x0;
  this.ySize = this.yF - this.y0;
  this.xDB = this.xD - this.xB;

  var widgets = [];
  this.upWidget = new htrace.TriangleButton({
    ctx: this.ctx,
    direction: "up",
    x0: this.xB + 2,
    xF: this.xB + (this.xDB / 2) - 2,
    y0: this.y0 + 2,
    yF: this.yF - 2,
  });
  widgets.push(this.upWidget);
  this.downWidget = new htrace.TriangleButton({
    ctx: this.ctx,
    direction: "down",
    x0: this.xB + (this.xDB / 2) + 2,
    xF: this.xD - 2,
    y0: this.y0 + 2,
    yF: this.yF - 2,
  });
  widgets.push(this.downWidget);
  this.widgetManager = new htrace.WidgetManager({
    widgets: widgets,
  });

  this.draw = function() {
    this.drawBackground();
    this.drawProcessId();
    this.drawDescription();
    this.widgetManager.draw();
  };

  // Draw the background of this span widget.
  this.drawBackground = function() {
    this.ctx.save();
    if (this.selected) {
      this.ctx.fillStyle="#ffccff";
    } else {
      this.ctx.fillStyle="#ffffff";
    }
    this.ctx.fillRect(this.x0, this.y0, this.xSize, this.ySize);
    this.ctx.restore();
  }

  // Draw process ID text.
  this.drawProcessId = function() {
    this.ctx.save();
    this.ctx.fillStyle="#000000";
    this.ctx.font = (this.ySize - 2) + "px sans-serif";
    this.ctx.beginPath();
    this.ctx.rect(this.x0, this.y0, this.xB - this.x0, this.ySize);
    this.ctx.clip();
    this.ctx.fillText(this.span.get('processId'), this.x0, this.yF - 4);
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

    var gapY = 2;
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
    }

    // Draw description text
    this.ctx.fillStyle="#000000";
    this.ctx.font = (this.ySize - gapY) + "px sans-serif";
    this.ctx.fillText(this.span.get('description'), this.xD, this.yF - gapY - 2);

    this.ctx.restore();
  };

  // Convert a time in milliseconds since the epoch to an x position.
  this.timeToPosition = function(time) {
    return this.xD +
      (((time - this.begin) * (this.xF - this.xD)) /
        (this.end - this.begin));
  };

  this.inBoundingBox = function(x, y) {
    return ((x >= this.x0) && (x <= this.xF) && (y >= this.y0) && (y <= this.yF));
  };

  this.handleMouseDown = function(x, y) {
    if (!this.inBoundingBox(x, y)) {
      return false;
    }
    if (this.widgetManager.handleMouseDown(x, y)) {
      this.widgetManagerFocused = true;
      return true;
    }
    this.selected = !this.selected;
    this.fillSpanDetailsView();
    return true;
  };

  this.handleMouseUp = function(x, y) {
    if (this.widgetManagerFocused) {
      this.widgetManager.handleMouseUp(x, y);
      this.widgetManagerFocused = false;
    }
  };

  this.handleMouseMove = function(x, y) {
    if (!this.widgetManagerFocused) {
      return false;
    }
    return this.widgetManager.handleMouseUp(x, y);
  };

  this.fillSpanDetailsView = function() {
    var info = {
      spanID: this.span.get("spanID"),
      begin: htrace.dateToString(parseInt(this.span.get("begin"), 10)),
      end: htrace.dateToString(parseInt(this.span.get("end"), 10))
    };
    var explicitOrder = {
      spanId: -3,
      begin: -2,
      end: -1
    };
    keys = [];
    for(k in this.span.attributes) {
      keys.push(k);
      if (info[k] == null) {
        info[k] = this.span.get(k);
      }
    }
    // We sort the keys so that the stuff we want at the top appears at the top,
    // and everything else is in alphabetical order.
    keys = keys.sort(function(a, b) {
        var oa = explicitOrder[a] || 0;
        var ob = explicitOrder[b] || 0;
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
      h += _.template('<tr bgcolor="' + colorString + '">' +
            '<td style="width:30%;word-wrap:break-word"><%- key %></td>' +
            '<td style="width:70%;word-wrap:break-word"><%- val %></td>' +
          "</tr>")({key: keys[i], val: info[keys[i]]});
    }
    h += '</table>';
    $("#spanDetails").html(h);
  };

  return this;
};
