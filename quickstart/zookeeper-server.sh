#!/bin/bash 

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

if [ $# -gt 1 ]; then
        echo "USAGE:   $0 [zookeeper_properties]"
        exit 1
elif [ $# -eq 0 ]; then
        ZK_CONFIG=$BASE_DIR/quickstart/config/zookeeper.properties
else
        ZK_CONFIG=$1
fi

echo "ZK_CONFIG=$ZK_CONFIG"

LIB_DIR="$BASE_DIR/venice-server/build/install/venice-server/lib/"

if [ -e $LIB_DIR ]; then
        echo "LIB_DIR exists: $LIB_DIR"
else
        echo "LIB_DIR does not exist: $LIB_DIR"
        echo "Did you forget to generate it? Run the following command to do so:"
        echo ""
        echo "./gradlew buildAll"
        exit 1
fi

CLASSPATH="$LIB_DIR/zookeeper-3.4.6.jar:$LIB_DIR/slf4j-log4j12-1.7.14.jar:$LIB_DIR/slf4j-api-1.7.14.jar:$LIB_DIR/slf4j-simple-1.7.12.jar:$LIB_DIR/log4j-1.2.17.jar"

BASE_SCRIPT="java -classpath $CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain"

cmd="$BASE_SCRIPT $ZK_CONFIG"
echo "Will execute the following command: $cmd"
exec $cmd

