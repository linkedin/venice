#!/bin/bash 

if [ $# -gt 2 ];
then
        echo "USAGE:   $0 [config_file_path]"
        exit 1
fi

projectName="venice-router"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

cmd="$baseScript $@"
exec $cmd