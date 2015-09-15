<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Building HTrace
## Publishing a Release Candidate
TODO

## Publishing htrace.incubator.apache.org website
Checkout the current website. It is in svn (that's right, subversion).
The website is in a distinct location such that when its' content is svn
committed, the commit is published as `htrace.incubator.apache.org`.

Here is how you'd check out the current site into a directory named
`htrace.incubator.apache.org` in the current directory:

 $ svn checkout https://svn.apache.org/repos/asf/incubator/htrace/site/publish htrace.incubator.apache.org

Next, run the site publishing script at `${HTRACE_CHECKOUT_DIR}/bin/publish_hbase_website.sh`.
It will dump out usage information that looks like this:
 $ ./bin/publish_hbase_website.sh
 Usage: ./bin/publish_hbase_website.sh [-i | -a] [-g &lt;dir>] [-s &lt;dir>]
 -h          Show this message
 -i          Prompts the user for input
 -a          Does not prompt the user. Potentially dangerous.
 -g          The local location of the HTrace git repository
 -s          The local location of the HTrace website svn checkout
Either -i or -a is required.
Edit the script to set default Git and SVN directories.

To run the publish site script interactively, here is an example where
the git checkout is at `~/checkouts/incubator-htrace` and the svn checkout
is at `~/checkouts/htrace.incubator.apache.org`:

 $ ./bin/publish_hbase_website.sh -i -g ~/checkouts/incubator-htrace -s ~/checkouts/htrace.incubator.apache.org/
