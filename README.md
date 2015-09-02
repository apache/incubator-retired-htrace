<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

HTrace
======
HTrace is a tracing framework for use with distributed systems.

See documentation at src/main/site/markdown/index.md or at http://htrace.incubator.apache.org.

Building
--------
Only Maven 3.0.4 should be used to create HTrace releases - see [HTRACE-236](https://issues.apache.org/jira/browse/HTRACE-236)
(Support building release artifacts with Maven versions other than 3.0.4) for details.  

To get around this while using Maven 3.3 [HTRACE-234](https://issues.apache.org/jira/browse/HTRACE-234)
you can run maven with `-DcreateDependencyReducedPom=false`.  

