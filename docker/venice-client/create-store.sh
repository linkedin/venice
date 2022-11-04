#!/bin/bash
url=$1
clusterName=$2
storeName=$3
keySchema=$4
valueSchema=$5

jar=/opt/venice/bin/venice-admin-tool-all.jar

# create store
java -jar $jar --new-store --url $url --cluster $clusterName  --store $storeName --key-schema-file $keySchema --value-schema-file $valueSchema

# update quota and enabled incremental push
java -jar $jar --update-store --url $url --cluster $clusterName  --store $storeName --storage-quota -1 --incremental-push-enabled true
