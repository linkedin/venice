#!/bin/bash
BASE_DIR="`dirname ${BASH_SOURCE[0]}`/.."

if [ $# -gt 4 ] || [ $# -lt 3 ]; then
	echo "Usage: $0 <fabric> <store_name> <key_string> [venice_cluster]"
	echo "Do not include any space in key_string, and replace all \" by \\\"."
	exit 1
else
	# Initalized essential variables
	FABRIC=$1
	STORE_NAME=$2
	KEY_STRING=$3
	D2_CLUSTER="VeniceRouter"
	if [ $# -eq 4 ]; then
		VENICE_CLUSTER=$4
	else
		VENICE_CLUSTER="test-cluster"
	fi
fi

echo "Find a Vence router in cluster: $D2_CLUSTER in fabirc: $FABRIC through D2..."
FIND_ROUTER_RESULT=`curli --fabric $FABRIC d2://d2Clusters/$D2_CLUSTER`
# choose a router from d2 cluster
ROUTER_URL=`echo $FIND_ROUTER_RESULT | jq '.uris[0].URI'`

echo "Send a request to $ROUTER_URL, Store $STORE_NAME, Key string: $KEY_STRING..."

java -jar $BASE_DIR/build/libs/venice-thin-client-0.1.jar $VENICE_CLUSTER $STORE_NAME $KEY_STRING $ROUTER_URL
