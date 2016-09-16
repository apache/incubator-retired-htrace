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
### Generating the PGP key (if you haven't already)
Create new pgp key.  See http://www.apache.org/dev/openpgp.html

    sudo zypper -y install gpg
    gpg --version
    gpg (GnuPG) 2.0.22

This is a new enough version according to the apache site.

Generate a new key via

    gpg --gen-key --s2k-digest-algo sha512

Answer 4096 bits, no expiration time.

Uploaded key to http://pgp.mit.edu/ It now appears as

    http://pgp.mit.edu/pks/lookup?op=get&amp;search=0xDE78987A9CD4D9D3

Get the pgp key signed by others.

Add the new pgp key to the KEYS file

### JIRA maintenance
From hadoop HowToRelease at https://wiki.apache.org/hadoop/HowToRelease :
Bulk update Jira to unassign from this release all issues that are open
non-blockers.

#### Maven and build
Update the versions in Maven.

    mvn versions:set -DnewVersion=4.0.0

or do it manually by editing the pom.xml files.  Commit the new versions.

Add a settings.xml in `~/.m2/settings.xml` with the appropriate logins.

Create the release branch:

   git checkout -b 4.0

Create an annotated (signed) release tag via git.

    git tag -s 4.0.0RC0 -m '4.0.0 release candidate 0'

For some reason, I get a message about needing a password.  But no password
is actually asked for.  Perhaps it is supplied by the window manager via its
keyring-like program.

Push branch-X.Y.Z and the newly created tag to the remote repo.

    git push apache 4.0 4.0.0RC0

Upload the build to Sonatype's servers.

    git clean -fdqx .
    mvn clean deploy -Pdist -DskipTests

The above referenced profiles are not in the htrace profile but in the
parent apache profile. Here is a pointer to the parent profile:
[Apache-18.pom](https://repository.apache.org/service/local/repo_groups/public/content/org/apache/apache/18/apache-18.pom)

This will take a while because it needs to upload to the servers.
It will ask you for your key password unless you set up a
gpg-agent for your local session.

Log into repository.apache.org and go to
https://repository.apache.org/#stagingRepositories
Select the repo you uploaded earlier.  You can browse the files
here to verify it looks right.  When you are satisfied, click "Close".

Generate the source tarball via:

    mvn clean install -DskipTests assembly:single -Pdist

Generate the side files.

     cd target
     gpg --print-mds htrace-4.0.0-incubating-src.tar.gz htrace-4.0.0-incubating-src.tar.gz.mds
     gpg --armor --output htrace-4.0.0-incubating-src.tar.gz.asc \
       --detach-sign htrace-4.0.0-incubating-src.tar.gz

sftp up to your public_html directory in home.apache.org

     cd target
     echo 'put htrace-4.0.0-incubating-src.tar.gz*' | \
       sftp home.apache.org:public_html/htrace-4.2.0-incubating-rc0

Generate release notes via JIRA at
https://issues.apache.org/jira/browse/HTRACE/
by clicking on the release and then clicking the grey "release notes"
button in the top right.

### Mailing list
Propose a new release on the mailing list.  Wait 1 day for responses.

Post a message with the form:

     > I've posted the first release candidate here:
     >
     > http://people.apache.org/~cmccabe/htrace/releases/4.0.0/rc0
     >
     > The jars have been staged here:
     >
     > https://repository.apache.org/content/repositories/orgapachehtrace-1017
     >
     > There's a lot of great stuff in this release [description]
     >
     > The vote will run for 5 days.
     >
     > cheers,
     > Colin
     >
     > [release notes]

## Publishing htrace.incubator.apache.org website
Checkout the current website. It is in svn (that's right, subversion).
The website is in a distinct location such that when its' content is svn
committed, the commit is published as `htrace.incubator.apache.org`.

Here is how you'd check out the current site into a directory named
`htrace.incubator.apache.org` in the current directory:

~~~
 $ svn checkout https://svn.apache.org/repos/asf/incubator/htrace/site/publish htrace.incubator.apache.org
~~~

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

~~~
 $ ./bin/publish_hbase_website.sh -i -g ~/checkouts/incubator-htrace -s ~/checkouts/htrace.incubator.apache.org/
~~~
