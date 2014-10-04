#!/bin/bash

USAGE="USAGE: venice-client.sh [--command] --key [key] --value [value]"

projectName="venice-client"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

# parse command line args
while [[ $1 != "" ]];
do

  case "$1" in

    --put) operation="put"; shift 1;;

    --delete) operation="delete"; shift 1;;     

    --get) operation="get"; shift 1;;

    --key|-k) key=$2; shift 2;;

    --value|-v) value=$2; shift 2;;

  esac
done

# Check that a valid operation has been given.
if [[ $operation == "" ]]
then
  echo "No operation has been defined! Exiting..."
  echo $USAGE
  exit 1
fi

# Check that a key exists
if [[ $key == "" ]]
then
  echo "No key has been defined! Exiting...";
  echo $USAGE
  exit 1
fi

# Check that if a put is given, a value exists
if [[ $operation == "put" && $value = "" ]]
then
  echo "Trying to execute a put, but no value given. Exiting...."
  echo $USAGE
  exit 1
fi

args="$operation $key $value"
cmd="$baseScript $args"

exec $cmd
