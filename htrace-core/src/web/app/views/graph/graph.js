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
  initialize: function(options) {
    options = options || {};

    if (!options.id) {
      console.error("GraphView requires argument 'id' to uniquely identify this graph.");
      return;
    }

    _.bindAll(this, "render");
    this.collection.bind('change', this.render);

    var links = this.links = [];
    var linkTable = this.linkTable = {};
    var nodes = this.nodes = [];
    var nodeTable = this.nodeTable = {};
    var force = this.force
        = d3.layout.force().size([$(window).width(), $(window).height() * 3/4])
                           .linkDistance($(window).height() / 5)
                           .charge(-120)
                           .gravity(0)
                           ;
    force.nodes(nodes)
         .links(links);

    force.on("tick", function(e) {
      var root = d3.select("#" + options.id);
      
      if (!root.node()) {
        return;
      }

      var selectedDatum = root.select(".selected").datum();

      // center selected node
      root.select("svg").attr("width", $(root.node()).width());
      selectedDatum.x = root.select("svg").attr("width") / 2;
      selectedDatum.y = root.select("svg").attr("height") / 2;

      // Push sources up and targets down to form a weak tree.
      var k = 10 * e.alpha;
      force.links().forEach(function(d, i) {
        d.source.y -= k;
        d.target.y += k;
      });

      var nodes = root.selectAll(".node").data(force.nodes());
      nodes.select("circle")
        .attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
      nodes.select("text")
        .attr("x", function(d) { return d.x - this.getComputedTextLength() / 2; })
        .attr("y", function(d) { return d.y; });
      root.selectAll(".link").data(force.links())
        .attr("d", function(d) {
          var start = {},
              end = {},
              angle = Math.atan2((d.target.x - d.source.x), (d.target.y - d.source.y));
          start.x = d.source.x + d.source.r * Math.sin(angle);
          end.x = d.target.x - d.source.r * Math.sin(angle);
          start.y = d.source.y + d.source.r * Math.cos(angle);
          end.y = d.target.y - d.source.r * Math.cos(angle);
          return "M" + start.x + " " + start.y
              + " L" + end.x + " " + end.y;
        });
    });
  },

  updateLinksAndNodes: function() {
    if (!this.spanId) {
      return;
    }

    var $this = this, collection = this.collection;

    var selectedSpan = this.collection.findWhere({
      "spanId": this.spanId
    });

    var findChildren = function(span) {
      var spanId = span.get("spanId");
      var spans = collection.filter(function(model) {
        return _(model.get("parents")).contains(spanId);
      });
      return _(spans).reject(function(span) {
        return span == null;
      });
    };
    var findParents = function(span) {
      var spans = _(span.get("parents")).map(function(parentSpanId) {
        return collection.findWhere({
          "spanId": parentSpanId
        });
      });
      return _(spans).reject(function(span) {
        return span == null;
      });
    };
    var spanToNode = function(span, level) {
      var table = $this.nodeTable;
      if (!(span.get("spanId") in table)) {
        table[span.get("spanId")] = {
          "name": span.get("spanId"),
          "span": span,
          "level": level,
          "group": 0,
          "x": parseInt($this.svg.attr('width')) / 2,
          "y": 250 + level * 50
        };
        $this.nodes.push(table[span.get("spanId")]);
      }

      return table[span.get("spanId")];
    };
    var createLink = function(source, target) {
      var table = $this.linkTable;
      var name = source.span.get("spanId") + "-" + target.span.get("spanId");
      if (!(name in table)) {
        table[name] = {
          "source": source,
          "target": target
        };
        $this.links.push(table[name]);
      }

      return table[name];
    };

    var parents = [], children = [];
    var selectedSpanNode = spanToNode(selectedSpan, 1);

    Array.prototype.push.apply(parents, findParents(selectedSpan));
    _(parents).each(function(span) {
      Array.prototype.push.apply(parents, findParents(span));
      createLink(spanToNode(span, 0), selectedSpanNode)
    });

    Array.prototype.push.apply(children, findChildren(selectedSpan));
    _(children).each(function(span) {
      Array.prototype.push.apply(children, findChildren(span));
      createLink(selectedSpanNode, spanToNode(span, 2))
    });
  },

  renderLinks: function(selection) {
    var path = selection.enter().append("path")
        .classed("link", true)
        .style("marker-end",  "url(#suit)");
    selection.exit().remove();
    return selection;
  },

  renderNodes: function(selection) {
    var $this = this;
    var g = selection.enter().append("g").attr("class", "node");
    var circle = g.append("circle")
      .attr("r", function(d) {
        if (!d.radius) {
          d.r = Math.log(d.span.duration());
         
          if (d.r > app.GraphView.MAX_NODE_SIZE) {
            d.r = app.GraphView.MAX_NODE_SIZE;
          }

          if (d.r < app.GraphView.MIN_NODE_SIZE) {
            d.r = app.GraphView.MIN_NODE_SIZE;
          }
        }

        return d.r;
      });
    var text = g.append("text").text(function(d) {
      return d.span.get("description");
    });

    selection.exit().remove();

    circle.on("click", function(d) {
      $this.setSpanId(d.name);
    });

    selection.classed("selected", null);
    selection.filter(function(d) {
      return d.span.get("spanId") == $this.spanId;
    }).classed("selected", true);
    
    return selection;
  },

  setSpanId: function(spanId) {
    var $this = this;
    this.spanId = spanId;

    this.updateLinksAndNodes();

    this.renderNodes(
      this.svg.selectAll(".node")
        .data(this.force.nodes(), function(d) {
          return d.name;
        }));

    this.renderLinks(
      this.svg.selectAll(".link")
        .data(this.force.links(), function(d) {
          return d.source.name + "-" + d.target.name;
        }));

    this.force.start();

    Backbone.history.navigate("!/spans/" + spanId);
    this.trigger("update:span", {"span": this.collection.findWhere({
      "spanId": spanId
    })});
  },

  render: function() {
    this.svg = d3.select(this.$el[0]).append("svg");
    this.svg.attr("height", 500)
       .attr("width", $(window).width())
       .attr("id", this.id);

    // Arrows
    this.svg.append("defs").selectAll("marker")
      .data(["suit", "licensing", "resolved"])
    .enter().append("marker")
      .attr("id", function(d) { return d; })
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 25)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
    .append("path")
      .attr("d", "M0,-5L10,0L0,5 L10,0 L0, -5")
      .style("stroke", "#4679BD")
      .style("opacity", "0.6");

    return this;
  }
});

app.GraphView.MAX_NODE_SIZE = 150;
app.GraphView.MIN_NODE_SIZE = 50;
