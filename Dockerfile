# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Apache HTrace Docker build environment
#

FROM ubuntu:trusty
MAINTAINER Apache HTrace <dev@htrace.incubator.apache.org>

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && apt-get dist-upgrade -y

# HTrace Dependencies
RUN apt-get install -y build-essential curl gcc git libleveldb-dev libsnappy-dev

# - Patchelf
RUN curl -sSL http://nixos.org/releases/patchelf/patchelf-0.8/patchelf-0.8.tar.bz2 | tar -C /tmp -xj && \
    cd /tmp/patchelf-*/ && \
    ./configure --prefix=/usr && \
    make install

# Java dependencies
RUN apt-get install -y software-properties-common && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    apt-get install -y oracle-java7-installer oracle-java7-set-default

RUN curl -sSL http://archive.apache.org/dist/maven/maven-3/3.0.4/binaries/apache-maven-3.0.4-bin.tar.gz | tar -C /usr/share -xz && \
    mv /usr/share/apache-maven-3.0.4 /usr/share/maven && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

RUN curl -sSL http://archive.apache.org/dist/ant/binaries/apache-ant-1.9.6-bin.tar.gz | tar -C /usr/share -xz && \
    mv /usr/share/apache-ant-1.9.6 /usr/share/ant && \
    ln -s /usr/share/ant/bin/ant /usr/bin/ant

ENV JAVA_HOME /usr/lib/jvm/java-7-oracle
ENV MAVEN_HOME /usr/share/maven
ENV ANT_HOME /ust/share/ant

# GO dependencies
RUN curl -sSL https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz | tar -C /usr/lib/ -xz && \
    mkdir -p /usr/share/go

ENV GOROOT /usr/lib/go
ENV GOPATH /usr/share/go
ENV PATH ${GOROOT}/bin:${GOPATH}/bin:$PATH

# Clean up
RUN apt-get clean && \
    rm -rf /var/cache/apt/* && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* && \
    rm -rf /var/tmp/*

WORKDIR $HOME
