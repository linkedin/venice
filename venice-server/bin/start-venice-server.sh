#!/bin/bash 

projectName="venice-server"  
USAGE="USAGE:\n$0 config.properties"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )" 
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

# Get config file
if [[ $1 != "" ]]
then
  configFile=$1;

else
  echo -e "Missing config file!"
  echo -e $USAGE
  exit 1
fi

cmd="$baseScript $1"

exec $cmd
