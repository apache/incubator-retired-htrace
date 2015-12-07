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

// Parse an ISO8601 date string into a moment.js object.
htrace.parseDate = function(val) {
  if (val.match(/^[0-9]([0-9]*)$/)) {
    // Treat an all-numeric field as UTC milliseconds since the epoch.
    return moment.utc(parseInt(val, 10));
  }
  // Look for approved date formats.
  var toTry = [
    "YYYY-MM-DDTHH:mm:ss,SSS",
    "YYYY-MM-DDTHH:mm:ss",
    "YYYY-MM-DDTHH:mm",
    "YYYY-MM-DD"
  ];
  for (var i = 0; i < toTry.length; i++) {
    var m = moment.utc(val, toTry[i], true);
    if (m.isValid()) {
      return m;
    }
  }
  throw "Please enter the date either as YYYY-MM-DDTHH:mm:ss,SSS " +
      "in UTC, or as the number of milliseconds since the epoch.";
};

// Convert a moment.js moment into an ISO8601-style date string.
htrace.dateToString = function(val) {
  return moment.utc(val).format("YYYY-MM-DDTHH:mm:ss,SSS");
};

// Normalize a span ID into the format the server expects to see--
// i.e. something like 00000000000000000000000000000000.
htrace.normalizeSpanId = function(str) {
  if (str.length != 32) {
    throw "The length of '" + str + "' was " + str.length +
      ", but span IDs must be 32 characters long.";
  }
  if (str.search(/[^0-9a-fA-F]/) != -1) {
    throw "Span IDs must contain only hexadecimal digits, but '" + str +
      "' contained invalid characters.";
  }
  return str;
};
