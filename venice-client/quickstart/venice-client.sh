#!/bin/bash

USAGE="USAGE\n$0 --get --key test_key --store test_store\n$0 --put --key test_key --value test_value --store test_store\n$0 --delete --key test_key --store test_store"

projectName="venice-client"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
baseScript=$base_dir"/build/install/"$projectName"/bin/"$projectName

interactive=false
store="test_shell_store"

# parse command line args
while [[ $1 != "" ]];
do

  case "$1" in

    --put) operation="put"; shift 1;;

    --delete) operation="delete"; shift 1;;     

    --get) operation="get"; shift 1;;

    --key|-k) key=$2; shift 2;;

    --value|-v) value=$2; shift 2;;

    --interactive|-i) interactive=true; shift 1;;

    --store|-s) store=$2; shift 2;; 

    *) echo "Input variable $1 is not recognized."; echo -e $USAGE; exit 1;

  esac
done

# Running in interactive console mode
if $interactive 
then
  echo "Entering the interactive shell."

  cmd=$baseScript
  exec $cmd  

else

  # Check that a valid operation has been given.
  if [[ $operation == "" ]]
  then
    echo "No operation has been defined! Exiting..."
    echo -e $USAGE
    exit 1
  fi

  # Check that a key exists
  if [[ $key == "" ]]
  then
    echo "No key has been defined! Exiting...";
    echo -e $USAGE
    exit 1
  fi

  # Check that if a put is given, a value exists
  if [[ $operation == "put" && $value = "" ]]
  then
    echo "Trying to execute a put, but no value given. Exiting...."
    echo -e $USAGE
    exit 1
  fi

  args="$store $operation $key $value"
  cmd="$baseScript $args"

  exec $cmd

fi
