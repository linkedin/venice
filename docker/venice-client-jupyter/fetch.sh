#!/bin/bash
router=$1
storeName=$2
key=$3
java -jar ~/bin/venice-thin-client-all.jar $storeName "$key" $router false ""  2>/dev/null
