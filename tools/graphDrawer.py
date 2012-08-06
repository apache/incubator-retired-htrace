#!/usr/bin/env python

  # Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at

  #     http://www.apache.org/licenses/LICENSE-2.0

  # Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import gv
from datetime import datetime
from pygraph.classes.graph import graph
from pygraph.classes.digraph import digraph
from pygraph.readwrite.dot import write

def spanTextLineToDict(stl):
    splitStl = stl.split("/<,")
    return {k:int(v) if k != "desc" else v for (k,v) in zip(spanDictKeys, splitStl)}

def buildGraph(nid):
    for child in pc[nid]:
        gr.add_node(child, [("label", nodesMap[child]["desc"] + "(" + str(nodesMap[child]["stop"] - nodesMap[child]["start"]) +  ")")])
        gr.add_edge((nid, child))
        buildGraph(child)

spanText = sys.stdin.read().strip("\\\\;;;;")
spanDictKeys = ["trace", "span", "parent", "start","stop","desc"]
spanText = spanText.split("\\\\;;;;")
nodes = [spanTextLineToDict(x) for x in spanText]
temp = sorted(spanText)
nodesMap = {s['span']:s for s in nodes}
pc = {}

for x in nodesMap.keys():
    if x not in pc:
        pc[x] = set()
    parentId = nodesMap[x]["parent"]
    if parentId not in pc:
        pc[parentId] = set()
    pc[parentId].add(x)

count = 0
for x in pc[0x74ace]:
    count += 1
    gr = digraph()
    gr.add_node(x, [("label", nodesMap[x]["desc"] + "(" + str(nodesMap[x]["stop"] - nodesMap[x]["start"]) +  ")")])
    buildGraph(x)
    dot = write(gr)
    gvv = gv.readstring(dot)
    gv.layout(gvv,'dot')
    gv.render(gvv,'png','./graphs/' + str(datetime.now()) + nodesMap[x]["desc"][:10] + '.png')

print("Created " + str(count)  + " images.")