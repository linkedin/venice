#!/bin/bash 

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 

source $BASE_DIR/quickstart/common-bash

set_base_script "venice-controller"

if [ $# -gt 2 ]; then
        echo "USAGE:   $0 [cluster_properties] [controller_properties] "
        exit 1
elif [ $# -eq 0 ]; then
	CLUSTER_CONFIG=$BASE_DIR/quickstart/config/cluster.properties
	CONTROLLER_CONFIG=$BASE_DIR/quickstart/config/controller.properties
else
        CLUSTER_CONFIG=$1
        CONTROLLER_CONFIG=$2
fi

echo "CLUSTER_CONFIG=$CLUSTER_CONFIG"
echo "CONTROLLER_CONFIG=$CONTROLLER_CONFIG"

cmd="$BASE_SCRIPT $CLUSTER_CONFIG $CONTROLLER_CONFIG"
exec $cmd
