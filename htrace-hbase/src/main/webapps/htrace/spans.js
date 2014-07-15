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

var traceid = window.location.search.substring(1).split("=")[1];
d3.json("/getspans/" + traceid, function(spans) {
    spans.map(function(s) {
        s.start = parseInt(s.start);
        s.stop = parseInt(s.stop);
      });
    var rootid = 477902;
    var margin = {top: 50, right: 500, bottom: 50, left: 50};
    var barheight = 20;
    var width = 800;
    var height = spans.length * barheight;
    var gleftmargin = 300;
    var tmin = d3.min(spans, function(s) { return s.start; });
    var tmax = d3.max(spans, function(s) { return s.stop; });
    var xscale = d3.time.scale()
      .domain([new Date(tmin), new Date(tmax)]).range([0, width]);
    var byparent = d3.nest()
      .sortValues(function(a, b) {
          return a.start < b.start ? -1 : a.start > b.start ? 1 : 0;
        })
      .key(function(e) {return e.parent_id})
      .map(spans, d3.map);
    addchildren(byparent.get(rootid), byparent);
    var sortedspans = [];
    traverse(byparent.get(rootid), function(e) { sortedspans.push(e); });

    var svg = d3.select("body").append("svg")
      .attr("width", width + gleftmargin + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var bars = svg.append("g")
      .attr("id", "bars")
      .attr("width", width)
      .attr("height", height)
      .attr("transform", "translate(" + gleftmargin + ", 0)");
    
    var span_g = bars.append("g")
      .selectAll("g")
      .data(sortedspans)
      .enter()
      .append("g")
      .attr("transform", function(s, i) {
          return "translate(0, " + (i * barheight + 5) + ")";
        });

    span_g.append("text")
      .text(function(s){ return s.process_id; })
      .style("alignment-baseline", "hanging")
      .attr("transform", "translate(" + (- gleftmargin) + ", 0)");

    var rect_g = span_g.append("g")
      .attr("transform", function(s) {
          return "translate(" + xscale(new Date(s.start)) + ", 0)";
        });

    rect_g.append("rect")
      .attr("height", barheight - 1)
      .attr("width", function (s) {
          return (width * (s.stop - s.start)) / (tmax - tmin) + 1;
        })
      .style("fill", "lightblue");

    rect_g.append("text")
      .text(function(s){ return s.description; })
      .style("alignment-baseline", "hanging");

    rect_g.append("text")
      .text(function(s){ return s.stop - s.start; })
      .style("alignment-baseline", "baseline")
      .style("text-anchor", "end")
      .style("font-size", "10px")
      .attr("transform", function(s, i) { return "translate(0, 10)"; });

    var axis = d3.svg.axis()
      .scale(xscale)
      .orient("top")
      .tickValues(xscale.domain())
      .tickFormat(d3.time.format("%x %X.%L"))
      .tickSize(6, 3);

    bars.append("g")
      .attr("class", "axis")
      .call(axis);
  });

function addchildren (nodes, byparent) {
  nodes.forEach(function(e) {
      if (byparent.get(e.span_id)) {
        e.children = byparent.get(e.span_id);
        addchildren(e.children, byparent);
      }
    });
}

function traverse (children, func) {
  children.forEach(function(e) {
      func(e);
      if (e.children) {
        traverse (e.children, func);
      }
    });
}
