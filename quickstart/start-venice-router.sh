#!/bin/bash 

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 

source $BASE_DIR/quickstart/common-bash

set_base_script "venice-router"

if [ $# -gt 1 ]; then
        echo "USAGE:   $0 [cluster_properties]"
        exit 1
elif [ $# -eq 0 ]; then
	CLUSTER_CONFIG=$BASE_DIR/quickstart/config/router.properties
else
	CLUSTER_CONFIG=$1
fi

echo "CLUSTER_CONFIG=$CLUSTER_CONFIG"

cmd="$BASE_SCRIPT $CLUSTER_CONFIG"
exec $cmd
