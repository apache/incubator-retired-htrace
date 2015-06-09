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

// Manages a set of widgets on the canvas.
// Buttons and sliders are both widgets.
htrace.WidgetManager = function(params) {
  this.widgets = [];
  this.focusedWidget = null;

  this.handleMouseDown = function(x, y) {
    if (this.focusedWidget != null) {
      this.focusedWidget = null;
    }
    var numWidgets = this.widgets.length;
    console.log("WidgetManager looking through " + numWidgets + " widgets.");
    for (var i = 0; i < numWidgets; i++) {
      if (this.widgets[i].handleMouseDown(x, y)) {
        this.focusedWidget = this.widgets[i];
        break;
      }
    }
    return (this.focusedWidget != null);
  };

  this.handleMouseUp = function(x, y) {
    if (this.focusedWidget != null) {
      this.focusedWidget.handleMouseUp(x, y);
      this.focusedWidget = null;
    }
  };

  this.handleMouseMove = function(x, y) {
    return this.focusedWidget != null ?
      this.focusedWidget.handleMouseMove(x, y) : false;
  };

  this.draw = function() {
    var numWidgets = this.widgets.length;
    for (var i = 0; i < numWidgets; i++) {
      this.widgets[i].draw();
    }
  };

  for (var k in params) {
    this[k]=params[k];
  }
  return this;
};
