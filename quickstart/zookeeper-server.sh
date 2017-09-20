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

deps=`ls $LIB_DIR | egrep "(zookeeper|slf4j|log4j)"`
for jar in $deps; do
    CLASSPATH="$CLASSPATH:$LIB_DIR/$jar"
done

BASE_SCRIPT="java -classpath $CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain"

cmd="$BASE_SCRIPT $ZK_CONFIG"
echo "Will execute the following command: $cmd"
exec $cmd

