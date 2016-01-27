#!/bin/bash 

if [ $# -gt 3 ];
then
        echo "USAGE:   $0 [helix_cluster_name] [zookeeper_address] [controller_name]"
        exit 1
fi

projectName="venice-controller"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

cmd="$baseScript $@"
exec $cmd