#!/bin/bash

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <storeName> <key>" >&2
  exit 1
fi

storeName=$1
key=$2
java -jar /opt/venice/bin/venice-client-all.jar "$storeName" "$key" zookeeper:2181 2>/dev/null
