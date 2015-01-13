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
# ./build.sh                Builds the code.
# ./build.sh test           Builds and runs all unit tests.
# ./build.sh bench          Builds and runs all benchmarks
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

case $ACTION in
clean)
    rm -rf -- "${GOBIN}" ${SCRIPT_DIR}/pkg
    find "${SCRIPT_DIR}/src/org/apache/htrace/resource" ! -name 'catalog.go' \
        -type f -exec rm -f {} +
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
    go run "$SCRIPT_DIR/src/org/apache/htrace/bundler/bundler.go" \
        --src="$SCRIPT_DIR/../web/" --dst="$SCRIPT_DIR/src/org/apache/htrace/resource/" \
            || die "bundler failed"

    # Discover the git version
    GIT_VERSION=$(git rev-parse HEAD)
    [ $? -eq 0 ] || GIT_VERSION="unknown"

    # Inject the release and git version into the htraced ldflags.
    FLAGS="-X main.RELEASE_VERSION ${RELEASE_VERSION} -X main.GIT_VERSION ${GIT_VERSION}"
    go install -ldflags "${FLAGS}" -v org/apache/htrace/... "$@"
    ;;
bench)
    go test -v org/apache/htrace/... -test.bench=. "$@"
    ;;
*)
    go ${ACTION} -v org/apache/htrace/... "$@"
    ;;
esac
