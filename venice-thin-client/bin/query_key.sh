#!/bin/bash
# We will eventually move this script out of open source repo to ECL once it's stable enough.
# By then, this script will download the latest thin-client jar from artifactory automatically,
# then execute the query command.
BASE_DIR="`dirname ${BASH_SOURCE[0]}`/.."

if [ $# -gt 4 ] || [ $# -lt 3 ]; then
	echo "Usage: $0 <fabric> <store_name> <key_string> [is_vson_store]"
	echo "Do not include any space in key_string, and replace all \" by \\\"."
	exit 1
else
	# Initialize essential variables
	FABRIC=$1
	STORE_NAME=$2
	KEY_STRING=$3
	IS_VSON_STORE="false"
	if [ $# -gt 3 ]; then
	  IS_VSON_STORE=$4
	fi
	D2_CLUSTER="VeniceRouter"
fi

# discover cluster for store
echo "Find the real d2 cluster for store: $STORE_NAME through in fabric: $FABRIC through d2..."
FIND_CLUSTER_RESULT=`curli --fabric $FABRIC d2://$D2_CLUSTER/discover_cluster/$STORE_NAME`
D2_CLUSTER=`echo $FIND_CLUSTER_RESULT | jq '.d2Service'| tr -d '"'`

echo "Find a Venice router in d2 cluster: $D2_CLUSTER in fabric: $FABRIC through D2..."
FIND_ROUTER_RESULT=`curli --fabric $FABRIC d2://d2Clusters/$D2_CLUSTER`
echo $FIND_ROUTER_RESULT
# choose a router from d2 cluster
ROUTER_URL=`echo $FIND_ROUTER_RESULT | jq '.uris[0].URI'`

echo ""
echo "Will send a request to $ROUTER_URL, Store $STORE_NAME, Key string: $KEY_STRING..."

java -jar $BASE_DIR/build/libs/venice-thin-client-0.1.jar $STORE_NAME $KEY_STRING $ROUTER_URL $IS_VSON_STORE
