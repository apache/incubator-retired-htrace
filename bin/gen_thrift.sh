#!/bin/bash
SOURCE="${BASH_SOURCE[0]}"
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

cd $DIR/../
FILES=htrace-zipkin/src/main/thrift/*
for FILE in $FILES
do
  thrift --gen java -out htrace-zipkin/src/main/java $FILE
done
