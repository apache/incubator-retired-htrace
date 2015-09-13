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

// Draws a vertical bar selecting a time.
htrace.TimeCursor = function(params) {
  this.positionToTime = function(x) {
    if ((x < this.x0) || (x > this.xF)) {
      return -1;
    }
    return this.begin +
      (((x - this.x0) * (this.end - this.begin)) / (this.xF - this.x0));
  };

  this.timeToPosition = function(time) {
    return this.x0 + (((time - this.begin) *
        (this.xF - this.x0)) / (this.end - this.begin));
  };

  this.draw = function() {
    if (this.selectedTime != -1) {
      this.ctx.save();
      this.ctx.beginPath();
      this.ctx.rect(this.x0, this.y0,
          this.xF - this.x0, this.yF - this.y0);
      this.ctx.clip();
      this.ctx.strokeStyle="#ff0000";
      var x = this.timeToPosition(this.selectedTime);
      this.ctx.beginPath();
      this.ctx.moveTo(x, this.y0);
      this.ctx.lineTo(x, this.yF);
      this.ctx.stroke();
      this.ctx.restore();
    }
  };

  this.handle = function(e) {
    switch (e.type) {
      case "mouseMove":
        if (htrace.inBoundingBox(e.x, e.y,
              this.x0, this.xF, this.y0, this.yF)) {
          this.selectedTime = this.positionToTime(e.x);
          if (this.selectedTime < 0) {
            $(this.el).val("");
          } else {
            $(this.el).val(htrace.dateToString(this.selectedTime));
          }
          return true;
        }
        return true;
      case "draw":
        this.draw();
        return true;
    }
  };

  this.selectedTime = -1;
  for (var k in params) {
    this[k]=params[k];
  }
  this.manager.register("mouseMove", this);
  this.manager.register("draw", this);
  return this;
};
