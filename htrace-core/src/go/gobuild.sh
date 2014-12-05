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

ACTION=get
if [ $# -gt 0 ]; then
    if [ "x${1}" == "xbench" ]; then
        # run benchmarks
        ACTION="test"
        set -- "$@" -test.bench=.
    else
        # run specified action
        ACTION="${1}"
    fi
    shift
fi

SCRIPT_DIR="$(cd "$( dirname $0 )" && pwd)"
export GOPATH="$GOPATH:${SCRIPT_DIR}"
export GOBIN="${SCRIPT_DIR}/bin"
mkdir -p ${GOBIN} || die "failed to create ${GOBIN}"

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
    ldconfig=ldconfig
fi
if "${ldconfig}" -p | grep -q libleveldb; then
    :
else
    echo "You must install the leveldb-devel package (or distro-specific equivalent.)"
    exit 1
fi

go "${ACTION}" -v org/apache/htrace/htraced org/apache/htrace/htrace "$@"
