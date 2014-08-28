/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

d3.json("/gettraces", function(spans) {
    spans.map(function(s) {
        s.start = parseInt(s.start);
        s.stop = parseInt(s.stop);
      });
    var table = d3.select("body")
      .append("table")
      .attr("style", "white-space: nowrap;")
      .attr("class", "table table-condensed");
    var thead = table.append("thead");    
    var tbody = table.append("tbody");
    var columns = ["start", "process_id", "description"];

    thead.append("tr")
      .selectAll("th")
      .data(columns)
      .enter()
      .append("th")
      .html(function(d) { return d; });

    var rows = tbody.selectAll("tr")
      .data(spans)
      .enter()
      .append("tr")
      .sort(function(a, b) {
          return a.start > b.start ? -1 : a.start < b.start ? 1 : 0;
        })
      .attr("onclick", function(d) {
          return "window.location='spans.html?traceid=" + d.trace_id + "'";
        });

    rows.selectAll("td")
      .data(function(s) {
          return columns.map(function(c) { return s[c]; });
        })
      .enter()
      .append("td")
      .html(function(d) { return d; });

    rows.select("td")
      .html(function(d) {
          return d3.time.format("%x %X.%L")(new Date(d.start));
        });
  });
