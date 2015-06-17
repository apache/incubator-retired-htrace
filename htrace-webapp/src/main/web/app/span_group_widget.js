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

// Widget containing a group of trace spans displayed on the canvas.
htrace.SpanGroupWidget = function(params) {
  this.draw = function() {
    this.ctx.save();
    this.ctx.fillStyle="#ffffff";
    this.ctx.fillRect(this.x0, this.y0, this.xF - this.x0, this.yF - this.y0);
    this.ctx.strokeStyle="#aaaaaa";
    this.ctx.beginPath();
    this.ctx.moveTo(this.x0, this.y0);
    this.ctx.lineTo(this.xF, this.y0);
    this.ctx.stroke();
    this.ctx.beginPath();
    this.ctx.moveTo(this.x0, this.yF);
    this.ctx.lineTo(this.xF, this.yF);
    this.ctx.stroke();
    this.ctx.restore();
    return true;
  };

  this.createSpanWidget = function(node, indentLevel,
      allowUpButton, allowDownButton) {
    new htrace.SpanWidget({
      manager: this.manager,
      ctx: this.ctx,
      span: node,
      x0: this.x0,
      xB: this.xB,
      xD: this.xD,
      xF: this.xF,
      xT: this.childIndent * indentLevel,
      y0: this.spanY,
      yF: this.spanY + this.spanWidgetHeight,
      allowUpButton: allowUpButton,
      allowDownButton: allowDownButton,
      begin: this.begin,
      end: this.end
    });
    this.spanY += this.spanWidgetHeight;
  }

  this.handle = function(e) {
    switch (e.type) {
      case "draw":
        this.draw();
        return true;
    }
  }

  for (var k in params) {
    this[k]=params[k];
  }
  this.manager.register("draw", this);
  this.spanY = this.y0 + 4;

  // Figure out how much to indent each child's description text.
  this.childIndent = Math.max(10, (this.xF - this.xD) / 50);

  // Get the maximum depth of the parents tree to find out how far to indent.
  var parentTreeHeight =
      htrace.treeHeight(this.span, htrace.getReifiedParents);

  console.log("parentTreeHeight = " + parentTreeHeight);
  // Traverse the parents tree upwards.
  var thisWidget = this;
  htrace.treeTraverseDepthFirstPost(this.span, htrace.getReifiedParents, 0,
      function(node, depth) {
        if (depth > 0) {
          thisWidget.createSpanWidget(node,
              parentTreeHeight - depth, true, false);
        }
      });
  thisWidget.createSpanWidget(this.span, parentTreeHeight, true, true);
  // Traverse the children tree downwards.
  htrace.treeTraverseDepthFirstPre(this.span, htrace.getReifiedChildren, 0,
      function(node, depth) {
        if (depth > 0) {
          thisWidget.createSpanWidget(node,
              parentTreeHeight + depth, false, true);
        }
      });
  this.yF = this.spanY + 4;
  console.log("SpanGroupWidget(this.span=" +
      JSON.stringify(this.span.unparse()) +
      ", x0=" + this.x0 + ", xB=" + this.xB +
      ", xD=" + this.xD + ", xF=" + this.xF +
      ", y0=" + this.y0 + ", yF=" + this.yF +
      ")");
  return this;
};
