#!/bin/bash 

projectName="venice-server"  

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

cmd="$baseScript"

exec $cmd
