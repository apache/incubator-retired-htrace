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

// Check if a point is inside a bounding box.
htrace.inBoundingBox = function(x, y, x0, xF, y0, yF) {
    return ((x >= x0) && (x <= xF) && (y >= y0) && (y <= yF));
  }

// Manages a set of widgets on the canvas.
// Buttons and sliders are both widgets.
htrace.WidgetManager = function(params) {
  this.listeners = {
    "mouseDown": [],
    "mouseUp": [],
    "mouseMove": [],
    "mouseOut": [],
    "draw": [],
  };

  this.register = function(type, widget) {
    this.listeners[type].push(widget);
  }

  this.unregister = function(type, widget) {
    this.listeners[type] = _.without(this.listeners[type], widget);
  }

  this.handle = function(e) {
    // Make a copy of the listeners, in case the handling functions change the
    // array.
    var listeners = this.listeners[e.type].slice();
    var len = listeners.length;
    for (var i = 0; i < len; i++) {
      if (!listeners[i].handle(e)) {
        break;
      }
    }
  };

  for (var k in params) {
    this[k]=params[k];
  }
  return this;
};
