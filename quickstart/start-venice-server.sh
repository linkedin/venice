#!/bin/bash 

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 

source $BASE_DIR/quickstart/common-bash

set_base_script "venice-server"

if [ $# -gt 1 ]; then
        echo "USAGE:   $0 [venice_config_dir]"
        exit 1
elif [ $# -eq 0 ]; then
	VENICE_CONFIG_DIR=$BASE_DIR/quickstart/config/
else
	VENICE_CONFIG_DIR=$1
fi

echo "VENICE_CONFIG_DIR=$VENICE_CONFIG_DIR"

cmd="$BASE_SCRIPT $VENICE_CONFIG_DIR"
exec $cmd
