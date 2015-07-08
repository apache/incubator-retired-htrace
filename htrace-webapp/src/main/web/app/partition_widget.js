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

// Widget containing draggable horizontal partition
htrace.PartitionWidget = function(params) {
  this.draw = function() {
    this.ctx.save();
    this.ctx.fillStyle = this.selected ? "#6600ff" : "#aaaaaa";
    this.ctx.fillRect(this.x0, this.y0, this.xF - this.x0, this.yF - this.y0);
    this.ctx.restore();
  };

  this.handle = function(e) {
    switch (e.type) {
      case "mouseDown":
        if (!htrace.inBoundingBox(e.x, e.y,
              this.x0, this.xF, this.y0, this.yF)) {
          return true;
        }
        this.manager.registerHighPriority("mouseUp", this);
        this.manager.registerHighPriority("mouseMove", this);
        this.selected = true;
        $(this.el).css('cursor', 'ew-resize');
        return false;
      case "mouseMove":
        // Move the partition to the mouse pointer area
        var x = Math.min(Math.max(e.x, this.xMin), this.xMax);
        var width = this.xF - this.x0;
        this.x0 = Math.floor(x - (width / 2));
        this.xF = this.x0 + width;
        return false;
      case "mouseUp":
        this.manager.unregister("mouseUp", this);
        this.manager.unregister("mouseMove", this);
        $(this.el).css('cursor', 'pointer');
        this.selected = false;
        this.releaseHandler(this.x0);
        return false;
      case "draw":
        this.draw();
        return true;
      default:
        return true;
    }
  };

  for (var k in params) {
    this[k]=params[k];
  }
  this.selected = false;
  this.manager.register("mouseDown", this);
  this.manager.register("draw", this);
  return this;
};
