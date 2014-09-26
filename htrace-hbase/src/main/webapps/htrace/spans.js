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
        if (s.timeline) {
          s.timeline.forEach(function(t) { t.time = parseInt(t.time); });
        }
      });
    var rootid = "477902";
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
      .key(function(e) { return e.parent_id; })
      .map(spans, d3.map);
    addchildren(byparent.get(rootid), byparent);
   
    var tree = d3.layout.tree()
      .sort(function(a, b) {
          return a.start < b.start ? -1 : a.start > b.start ? 1 : 0;
        });
    var sortedspans = tree.nodes(byparent.get(rootid)[0]);

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
    
    var span_g = bars.selectAll("g.span")
      .data(sortedspans)
      .enter()
      .append("g")
      .attr("transform", function(s, i) {
          return "translate(0, " + (i * barheight + 5) + ")";
        })
      .classed("timeline", function(d) { return d.timeline; });

    span_g.append("text")
      .text(function(s) { return s.process_id; })
      .style("alignment-baseline", "hanging")
      .attr("transform", function(s) {
          return "translate(" + (s.depth * 10 - gleftmargin) + ", 0)";
        });

    var rect_g = span_g.append("g")
      .attr("transform", function(s) {
          return "translate(" + xscale(new Date(s.start)) + ", 0)";
        });

    rect_g.append("rect")
      .attr("height", barheight - 1)
      .attr("width", function (s) {
          return (width * (s.stop - s.start)) / (tmax - tmin) + 1;
        })
      .style("fill", "lightblue")
      .attr("class", "span")

    rect_g.append("text")
      .text(function(s){ return s.description; })
      .style("alignment-baseline", "hanging");

    rect_g.append("text")
      .text(function(s){ return s.stop - s.start; })
      .style("alignment-baseline", "baseline")
      .style("text-anchor", "end")
      .style("font-size", "10px")
      .attr("transform", function(s, i) { return "translate(0, 10)"; });

    bars.selectAll("g.timeline").selectAll("rect.timeline")
      .data(function(s) { return s.timeline; })
      .enter()
      .append("rect")
      .style("fill", "red")
      .attr("height", 8)
      .attr("width", 8)
      .attr("transform", function(t) {
          return "translate(" + xscale(t.time) + ", 11)";
        })
      .classed("timeline");

    var popup = d3.select("body").append("div")
      .attr("class", "popup")
      .style("opacity", 0);

    bars.selectAll("g.timeline")
      .on("mouseover", function(d) {
          popup.transition().duration(300).style("opacity", .95);
          var text = "<table>";
          d.timeline.forEach(function (t) {
              text += "<tr><td>" + (t.time - tmin) + "</td>";
              text += "<td> : " + t.message + "<td/></tr>";
            });
          text += "</table>"
          popup.html(text)
            .style("left", "30px")
            .style("top", (document.body.scrollTop + 30) + "px")
            .style("width", "700px")
            .style("background", "orange")
            .style("position", "absolute");
        })
      .on("mouseout", function(d) {
          popup.transition().duration(300).style("opacity", 0);
        });
    
    var axis = d3.svg.axis()
      .scale(xscale)
      .orient("top")
      .tickValues(xscale.domain())
      .tickFormat(d3.time.format("%x %X.%L"))
      .tickSize(6, 3);

    bars.append("g").attr("class", "axis").call(axis);
  });

function addchildren (nodes, byparent) {
  nodes.forEach(function(e) {
      if (byparent.get(e.span_id)) {
        e.children = byparent.get(e.span_id);
        addchildren(e.children, byparent);
      }
    });
}
