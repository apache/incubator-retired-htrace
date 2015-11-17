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

htrace.ServerInfoView = Backbone.View.extend({
  events: {
    "click .serverConfigurationButton": "showServerConfigurationModal",
    "click .storageDirectoryStatsButton": "showStorageDirectoryStatsModal",
    "click .debugInfoButton": "showDebugInfoModal",
  },

  render: function() {
    this.$el.html(_.template($("#server-info-view-template").html())
      ({ model : this.model,
         view : this}));
    console.log("ServerInfoView#render");
    return this;
  },

  close: function() {
    console.log("ServerInfoView#close")
    this.undelegateEvents();
  },

  getServerStatsTableHtml: function() {
    var out = 
      '<div class="panel-heading">' + 
        '<div class="panel-title">Span Metrics</div>' +
      '</div>' +
      '<table class="table table-striped">' +
        '<thead>' +
          '<tr>' +
            '<th>Remote</th>' +
            '<th>Written</th>' +
            '<th>ServerDropped</th>' +
            '<th>ClientDropped</th>' +
          '</tr>' +
        '</thead>';
    var remotes = [];
    var stats = this.model.stats
    var spanMetrics = stats.get("HostSpanMetrics")
    for (var remote in spanMetrics) {
      if (spanMetrics.hasOwnProperty(remote)) {
        remotes.push(remote)
      }
    }
    remotes = remotes.sort()
    for (var i = 0; i < remotes.length; i++) {
      var remote = remotes[i];
      var smtx = spanMetrics[remote];
      out = out + "<tr>" + 
        "<td>" + remote + "</td>" +
        "<td>" + smtx.Written + "</td>" +
        "<td>" + smtx.ServerDropped + "</td>" +
        "<td>" + smtx.ClientDropped + "</td>" +
        "</tr>";
    }
    out = out + '</table>';
    //console.log("out = " + out);
    return out;
  },

  showServerConfigurationModal: function() {
    var config = new htrace.ServerConfiguration();
    var view = this;
    config.fetch({
      "error": function(model, response, options) {
        window.alert("Failed to fetch htraced server configuration: " +
                     JSON.stringify(response));
      },
      "success": function(model, response, options) {
        var confKeys = [];
        for (var confKey in model.attributes) {
          confKeys.push(confKey)
        }
        console.log("confKeys = " + JSON.stringify(confKeys));
        confKeys = confKeys.sort()
        var out = '<table style="table-layout:fixed;width:100%;word-wrap:break-word">' +
            '<thead>' +
              '<tr>' +
                '<th>Key</th>' +
                '<th>Value</th>' +
              '</tr>' +
            '</thead>';
        for (var i = 0; i < confKeys.length; i++) {
          var colorString = ((i%2) == 1) ? "#f1f1f1" : "#ffffff";
          out += _.template($("#table-row-template").html())(
              {bgcolor: colorString, key: confKeys[i],
                val: model.attributes[confKeys[i]]});
          //out = out + '<tr><th>' + confKeys[i] + '</th><th>' +
              //model.attributes[confKeys[i]] + '</th></tr>';
        }
        var out = out + '</table>';
        htrace.showModal(_.template($("#modal-table-template").html())(
            {title: "HTraced Server Configuration", body: out}));
      }
    })
  },

  showStorageDirectoryStatsModal: function() {
    var dirs = this.model.stats.get("Dirs");
    var out = "";
    for (var dirIdx = 0; dirIdx < dirs.length; dirIdx++) {
      var dir = dirs[dirIdx];
      out += "<h3>" + dir.Path + "</h3>";
      out += "Approximate size in bytes: " + dir.ApproximateBytes + "<br/>";
      out += "<pre>" + dir.LevelDbStats + "</pre></pre><br/><p/>";
    }
    htrace.showModal(_.template($("#modal-table-template").html())(
          {title: "HTraced Storage Directory Statistics", body: out}));
  },

  showDebugInfoModal: function() {
    var config = new htrace.ServerDebugInfo();
    var view = this;
    config.fetch({
      "error": function(model, response, options) {
        window.alert("Failed to fetch htraced server debug info: " +
                     JSON.stringify(response));
      },
      "success": function(model, response, options) {
        var out = "";
        out += "<h2>Stack Traces</h2>";
        out += "<pre>" + model.get("StackTraces") + "</pre>";
        out += "<h2>GC Stats</h2>";
        out += "<pre>" + model.get("GCStats") + "</pre>";
        htrace.showModal(_.template($("#modal-table-template").html())(
            {title: "HTraced Debug Info", body: out}));
      }
    })
  }
});
