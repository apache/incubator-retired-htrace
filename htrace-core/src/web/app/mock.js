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
 
var spans = new App.Spans([
  {"beginTime": 1419977682, "stopTime": 1419977689, "description": "test1()", "spanId": 1, "traceId": 1, "parentSpanId": null, "processId": "namenode:1"},
  {"beginTime": 1419977685, "stopTime": 1419977690, "description": "test2()", "spanId": 2, "traceId": 2, "parentSpanId": 1, "processId": "datanode:1"}
]);
