#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Builds the HTrace server code.
#
# ./gobuild.sh                Builds the code.
# ./gobuild.sh test           Builds and runs all unit tests.
# ./gobuild.sh bench          Builds and runs all benchmarks
#

die() {
    echo $@
    exit 1
}

ACTION=install
if [ $# -gt 0 ]; then
    ACTION="${1}"
    shift
fi
RELEASE_VERSION=${RELEASE_VERSION:-unknown}

# Set up directories.  The build/ directory is where build dependencies and
# build binaries should go.
SCRIPT_DIR="$(cd "$( dirname $0 )" && pwd)"
export GOBIN="${SCRIPT_DIR}/build"
mkdir -p "${GOBIN}" || die "failed to mkdir -p ${GOBIN}"
cd "${GOBIN}" || die "failed to cd to ${SCRIPT_DIR}"
export GOPATH="${GOBIN}:${SCRIPT_DIR}"

# Use the unsafe package when possible to get greater speed.  For example,
# go-codec can bypass the overhead of converting between []byte and string in
# some cases when using unsafe.
TAGS="-tags unsafe"

# Check for go
which go &> /dev/null
if [ $? -ne 0 ]; then
    cat <<EOF
You must install the Golang programming language.

If you are using Debian, try "apt-get install golang".
For Red Hat, try "yum install go".
For other distributions and operating systems use your packaging tool.
EOF
exit 1
fi

# Check for libleveldb.so
if [ -n "$LEVELDB_PREFIX" ]; then
    echo "using LEVELDB_PREFIX=$LEVELDB_PREFIX"
    export CGO_CFLAGS="-I${LEVELDB_PREFIX}/include"
    export CGO_LDFLAGS="-L${LEVELDB_PREFIX}"
else
    if [ -x "/sbin/ldconfig" ]; then
        # Suse requires ldconfig to be run via the absolute path
        ldconfig=/sbin/ldconfig
    else
        which ldconfig &> /dev/null
        [ $? -eq 0 ] && ldconfig=ldconfig
    fi
    if [ -n "${ldconfig}" ]; then
        if "${ldconfig}" -p | grep -q libleveldb; then
            :
        else
            echo "You must install the leveldb-devel package (or distro-specific equivalent.)"
            exit 1
        fi
    fi
fi

case $ACTION in
clean)
    rm -rf -- "${GOBIN}" ${SCRIPT_DIR}/pkg
    ;;
install)
    # Ensure that we have the godep program.
    PATH="${PATH}:${GOBIN}"
    which godep &> /dev/null
    if [ $? -ne 0 ]; then
        echo "Installing godep..."
        go get github.com/tools/godep || die "failed to get godep"
    fi

    # Download dependencies into the build directory.
    echo "godep restore..."
    godep restore || die "failed to set up dependencies"

    # Discover the git version
    GIT_VERSION=$(git rev-parse HEAD)
    [ $? -eq 0 ] || GIT_VERSION="unknown"

    # Inject the release and git version into the htraced ldflags.
    FLAGS="-X main.RELEASE_VERSION ${RELEASE_VERSION} -X main.GIT_VERSION ${GIT_VERSION}"
    go install ${TAGS} -ldflags "${FLAGS}" -v org/apache/htrace/... "$@"
    # Make a symlink to web src dir so can do development in-situ out
    # of build dir. This is ugly but blame go build.
    ln -fs "../../htrace-webapp/src/main/web" "${GOBIN}/../"
    ;;
bench)
    go test org/apache/htrace/... ${TAGS} -test.bench=. "$@"
    ;;
*)
    go ${ACTION} org/apache/htrace/... ${TAGS} "$@"
    ;;
esac
