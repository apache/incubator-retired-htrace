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
 
App.SearchView = Backbone.View.extend({
  "events": {
    "click a": "search",
    "click button": "search"
  },

  "search": function() {
    var now = new moment();
    var begin, stop;

    var description = $(this.el).find("input[type='search']").val();
    var begindate = $(this.el).find("#begindate").val();// || now.format("D MMMM, YYYY");
    var begintime = $(this.el).find("#begintime").val() || now.format("H:mm A");
    var enddate = $(this.el).find("#stopdate").val();// || now.format("D MMMM, YYYY");
    var endtime = $(this.el).find("#stoptime").val() || now.format("H:mm A");
    var duration = $(this.el).find("#duration").val();

    if (begindate) {
      begin = new moment(begindate + " " + begintime).unix();
    }

    if (enddate) {
      stop = new moment(enddate + " " + endtime).unix();
    }

    var predicates = [];

    if (begin) {
      predicates.push({
        "op": "ge",
        "field": "begin",
        "val": begin.toString()
      });
    }

    if (stop) {
      predicates.push({
        "op": "le",
        "field": "end",
        "val": stop.toString()
      });
    }

    if (duration) {
      predicates.push({
        "op": "ge",
        "field": "duration",
        "val": duration.toString()
      });
    }

    if (description) {
      predicates.push({
        "op": "cn",
        "field": "description",
        "val": description
      });
    }

    this.collection.query(null, predicates);

    return false;
  }
});
