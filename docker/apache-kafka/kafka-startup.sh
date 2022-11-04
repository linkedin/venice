#!/usr/bin/env bash

set -x
set -o nounset \
    -o errexit

# add zookeeper address to the properties file
echo "zookeeper.connect=$ZOOKEEPER_ADDRESS" >> config/server.properties
# start kafka server
bash bin/kafka-server-start.sh config/server.properties 
