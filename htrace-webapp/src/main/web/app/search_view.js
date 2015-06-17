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
htrace.SearchView = Backbone.View.extend({
  initialize : function() {
    this.predicateViews = [];
    this.highestPredicateIndex = 0;
    this.searchInProgress = false;
    this.searchResults = new htrace.SearchResults();
    this.resultsView = new htrace.SearchResultsView({
        searchResults: this.searchResults,
        el: "#results"
    });
  },

  events: {
    "click #searchButton": "searchHandler",
    "click #clearButton": "clearHandler",
    "click .add-field": "dropdownHandler",
    "blur #begin": "blurBeginHandler",
    "blur #end": "blurEndHandler",
    "click #zoomButton": "zoomFitAllHandler"
  },

  searchHandler: function(e){
    e.preventDefault();

    // Do a new search.
    this.doSearch(e.ctrlKey);
  },

  clearHandler: function(e){
    e.preventDefault();

    // Clear existing search results.
    this.searchResults.reset();
  },

  doSearch: function(showDebug){
    if (this.searchInProgress) {
      console.log("Can't start a new search while another one is in " +
          "progress.");
      return false;
    }

    // Check if there are no search criteria.
    if (this.predicateViews.length == 0) {
      htrace.showModalWarning("No Search Criteria Specified",
        "You have not specified any search criteria.  " +
        "Use the 'Add Predicate' button to specify what to search for.");
      return false;
    }

    // Build the predicate array.
    predicates = []
    var predicateViewsLen = this.predicateViews.length;
    for (var i = 0; i < predicateViewsLen; i++) {
      var predicateView = this.predicateViews[i];
      try {
        predicates.push(predicateView.getPredicate());
      } catch(err) {
        htrace.showModalWarning("Search Field Validation Error",
          "Invalid search string for the '" + predicateView.ptype.name +
          "' field.<p/>" + err);
        return false;
      }
    }
    var queryJson = {
      pred: predicates,
      lim: 20
    };
    // If there are existing search results, we want results which "come after"
    // those.  So pass the last span we saw as a continuation token.
    if (this.searchResults.size() > 0) {
      queryJson.prev =
          this.searchResults.at(this.searchResults.size() - 1).unparse();
    }
    var searchView = this;
    var queryResults = new htrace.QueryResults({queryJson: queryJson});
    console.log("Starting span query " + queryResults.url());
    this.searchInProgress = true;
    queryResults.fetch({
      success: function(model, response, options){
        var firstResults = (searchView.searchResults.size() === 0);
        console.log("Success on span query " + queryResults.url() + ": got " +
            queryResults.size() + " result(s).  firstResults=" + firstResults);
        searchView.searchResults.add(queryResults.models);
        if (firstResults) {
          // After the initial search, zoom to fit everything.
          // On subsequent searches, we leave the viewport alone.
          searchView.resultsView.zoomFitAll();
        }
        searchView.searchInProgress = false;
        if (showDebug) {
          htrace.showModalWarning("Search Debug",
            "This is the search debug box, accessible by holding down the " +
            "control key while clicking the search button.<p/>" +
            "<h3>Query JSON</h3><pre>" + queryResults.prettyQueryString() +
            "</pre><p/><h3>Response JSON</h3><pre>" +
            JSON.stringify(queryResults, null, 2) + "</pre><p/>");
        } else if (queryResults.size() == 0) {
          if (firstResults) {
            htrace.showModalWarning("No Results Found",
              "No results were found for your query.<p/>");
          } else {
            htrace.showModalWarning("No Additional Results Found",
              "No additional results were found for your query.<p/>");
          }
        }
      },
      error: function(model, response, options){
        searchView.searchResults.reset();
        var err = "Error " + JSON.stringify(response, null, 2) +
          " on span query " + queryResults.url();
        console.log(err);
        alert(err);
        searchView.searchInProgress = false;
      }
    });
    return false;
  },

  dropdownHandler: function(e){
    e.preventDefault();
    var text = $(e.target).text();
    var ptype = htrace.parsePType(text);
    if (!ptype) {
      alert("Unable to parse predicate type '" + text + "'");
      return false;
    }
    var index = this.highestPredicateIndex;
    this.highestPredicateIndex++;
    var el = "pred" + index;
    $("#predicates").append('<div id="' + el + '"/></div>');
    predicateView = new htrace.PredicateView({
      el: "#" + el,
      index: index,
      ptype: ptype,
      searchView: this
    });
    this.predicateViews.push(predicateView);
    predicateView.render();
    return true;
  },

  blurBeginHandler: function(e) {
    return this.resultsView.handleBeginOrEndChange(e, "begin");
  },

  blurEndHandler: function(e) {
    return this.resultsView.handleBeginOrEndChange(e, "end");
  },

  zoomFitAllHandler: function(e) {
    e.preventDefault();
    this.resultsView.zoomFitAll();
  },

  removePredicateView: function(predicateView) {
    this.predicateViews = _.without(this.predicateViews, predicateView);
  },

  render: function() {
    this.$el.html(_.template($("#search-view-template").html())
      ({ model : this.model }))
    this.resultsView.render();
    console.log("SearchView#render");
    return this;
  },

  close: function() {
    console.log("SearchView#close")
    while (this.predicateViews.length > 0) {
      this.predicateViews[0].remove();
    }
    this.resultsView.remove();
    this.resultsView = null;
    this.undelegateEvents();
  }
});
