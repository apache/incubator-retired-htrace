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

// Triangle button widget.
htrace.TriangleButton = function(params) {
  this.fgColor = "#6600ff";
  this.bgColor = "#ffffff";
  this.selected = false;
  this.direction = "down";

  this.draw = function() {
    this.ctx.save();
    var fg = this.selected ? this.bgColor : this.fgColor;
    var bg = this.selected ? this.fgColor : this.bgColor;
    this.ctx.beginPath();
    this.ctx.rect(this.x0, this.y0,
        this.xF - this.x0, this.yF - this.y0);
    this.ctx.clip();
    this.ctx.fillStyle = bg;
    this.ctx.strokeStyle = fg;
    this.ctx.fillRect(this.x0, this.y0,
        this.xF - this.x0, this.yF - this.y0);
    this.ctx.lineWidth = 3;
    this.ctx.strokeRect(this.x0, this.y0,
        this.xF - this.x0, this.yF - this.y0);
    var xPad = (this.xF - this.x0) / 5;
    var yPad = (this.yF - this.y0) / 5;
    this.ctx.fillStyle = fg;
    this.ctx.strokeStyle = fg;
    this.ctx.beginPath();
    this.ctx.strokeStyle = fg;
    if (this.direction === "up") {
      this.ctx.moveTo(Math.floor(this.x0 + ((this.xF - this.x0) / 2)),
          this.y0 + yPad);
      this.ctx.lineTo(this.xF - xPad, this.yF - yPad);
      this.ctx.lineTo(this.x0 + xPad, this.yF - yPad);
    } else if (this.direction === "down") {
      this.ctx.moveTo(this.x0 + xPad, this.y0 + yPad);
      this.ctx.lineTo(this.xF - xPad, this.y0 + yPad);
      this.ctx.lineTo(Math.floor(this.x0 + ((this.xF - this.x0) / 2)),
          this.yF - yPad);
    } else {
      console.log("TriangleButton: unknown direction " + this.direction);
    }
    this.ctx.closePath();
    this.ctx.fill();
    this.ctx.restore();
  };

  this.handle = function(e) {
    switch (e.type) {
      case "mouseDown":
        if (!htrace.inBoundingBox(e.x, e.y,
              this.x0, this.xF, this.y0, this.yF)) {
          return true;
        }
        this.manager.register("mouseUp", this);
        this.manager.register("mouseMove", this);
        this.manager.register("mouseOut", this);
        this.selected = true;
        return false;
      case "mouseUp":
        if (this.selected) {
          this.callback(e);
          this.selected = false;
        }
        this.manager.unregister("mouseUp", this);
        this.manager.unregister("mouseMove", this);
        this.manager.unregister("mouseOut", this);
        return true;
      case "mouseMove":
        this.selected = htrace.inBoundingBox(e.x, e.y,
                this.x0, this.xF, this.y0, this.yF);
        return true;
      case "mouseOut":
        this.selected = false;
        return true;
      case "draw":
        this.draw();
        return true;
    }
  };

  for (var k in params) {
    this[k]=params[k];
  }
  this.manager.register("mouseDown", this);
  this.manager.register("draw", this);
  return this;
};
