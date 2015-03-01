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

app.GraphView = Backbone.View.extend({
  "MAX_NODE_SIZE": 100,
  "MIN_NODE_SIZE": 30,

  initialize: function(options) {
    options = options || {};
    _.bindAll(this, "render");
    this.collection.bind('change', this.render);

    if (!options.id) {
      console.error("GraphView requires argument 'id' to uniquely identify this graph.");
      return;
    }
    if (!options.el) {
      console.error("GraphView requires argument 'el' to bind the graph to.");
      return;
    }
    if (!options.spanId) {
      console.error("GraphView requires argument 'spanId' as a start point.");
      return;
    }

    this.spanId = options.spanId;

    window.force = this.force
        = d3.layout.force().size([1000, 1000])
                           .alpha(0.1)
                           .linkDistance(this.MAX_NODE_SIZE*2)
                           .charge(-600)
                           .linkDistance(150)
                           .linkStrength(1)
                           .friction(0.2)
                           // .gravity(0.1)
                           ;

    force.on("tick", function(e) {
      var root = d3.select("#" + options.id);

      // Push sources up and targets down to form a weak tree.
      var k = 10 * e.alpha;
      force.links().forEach(function(d, i) {
        d.source.y -= k;
        d.target.y += k;
      });

      // set selected node in the middle.
      // var selectedNode = force.nodes()[0];
      // selectedNode.x = 500;
      // selectedNode.y = 500;

      var nodes = root.selectAll(".node").data(force.nodes());
      nodes.select("circle")
        .attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
      nodes.select("text")
        .attr("x", function(d) { return d.x; })
        .attr("y", function(d) { return d.y; });
      root.selectAll(".link").data(force.links())
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });
    });
  },

  parents: function(span) {
    var collection = this.collection;
    return _(span.get("parents")).map(function(parentSpanId) {
      collection.findWhere({
        "spanId": parentSpanId
      });
    });
  },

  children: function(span) {
    var spanId = span.get("spanId");
    return this.collection.filter(function(model) {
      return _(model.get("parents")).contains(spanId);
    });
  },

  linksAndNodes: function() {
    var links = [],
        nodes = [];
    var selectedSpan = this.collection.findWhere({
      "spanId": this.spanId
    });
    var parents = this.parents(selectedSpan);
    var children = this.children(selectedSpan);

    var group = 0;
    var spanToNode = (function() {
      var xmap = {};
      return function(span, level) {
        return {
          "name": span.get("description"),
          "span": span
        };
      }
    })();
    var createLink = function(source, target) {
      return {
        "source": source,
        "target": target
      };
    };

    var selectedSpanNode = spanToNode(selectedSpan, 1);
    nodes.push(selectedSpanNode);

    _(parents).each(function(span) {
      var node = spanToNode(span, 0);
      nodes.push(node);
      links.push(createLink(node, selectedSpanNode));
    });

    _(children).each(function(span) {
      var node = spanToNode(span, 2);
      nodes.push(node);
      links.push(createLink(selectedSpanNode, node));
    });

    return {
      "links": links,
      "nodes": nodes
    };
  },

  renderLinks: function(selection) {
    selection.append("line")
        .attr("class", "link");
    return selection;
  },

  renderNodes: function(selection) {
    var MAX_NODE_SIZE = this.MAX_NODE_SIZE,
        MIN_NODE_SIZE = this.MIN_NODE_SIZE;
    var g = selection.append("g").attr("class", "node");
    g.append("circle")
      .attr("r", function(d) {
        var reduced = Math.log(d.span.duration());
         
        if (reduced > MAX_NODE_SIZE) {
          return MAX_NODE_SIZE;
        }

        if (reduced < MIN_NODE_SIZE) {
          return MIN_NODE_SIZE;
        }

        return reduced;
      });
    var text = g.append("text").text(function(d) {
      return d.name;
    });
    
    return selection;
  },

  render: function() {
    var svg = d3.select(this.$el[0]).append("svg")
      .attr("height", 1000)
      .attr("width", 1000)
      .attr("id", this.id);
    var data = this.linksAndNodes();

    this.force
      .nodes(data.nodes)
      .links(data.links)
      .start();

    var link = this.renderLinks(
      svg.selectAll(".link")
        .data(this.force.links())
        .enter());

    var node = this.renderNodes(
      svg.selectAll(".node")
        .data(this.force.nodes())
        .enter());

    return this;
  }
});
