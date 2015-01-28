#!/bin/bash 

if [ $# -gt 2 ];
then
        echo "USAGE:   $0 [venice_home] [venice_config_dir]"
        exit 1
fi

projectName="venice-server"  

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

cmd="$baseScript $@"
exec $cmd
