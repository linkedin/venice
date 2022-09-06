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

SSL_CONFIG="$BASE_DIR/config/sslconfig.properties"

# Validate certificate
if ! grep -q "keystore.password=" $SSL_CONFIG; then
  echo "ERROR: $SSL_CONFIG does not contain keystore.password"
  exit 1
elif ! grep -q "keystore.path=" $SSL_CONFIG; then
  echo "ERROR: $SSL_CONFIG does not contain keystore.path"
  exit 1
fi

keystorePassword=`grep "keystore.password=" $SSL_CONFIG | cut -d= -f2-`
keystorePath=`grep "keystore.path=" $SSL_CONFIG | cut -d= -f2-`
keystoreRealPath=`python -c "import os; print os.path.realpath('$keystorePath')"`

if [[ "$keystorePath" != "$keystoreRealPath" ]]; then
  echo "ERROR: Please use absolute path for $keystorePath"
  exit 1
elif [[ ! -e $keystorePath ]]; then
  echo "ERROR: $keystorePath does not exist"
  exit 1
else
  notAfter=$(2>/dev/null openssl pkcs12 -in $keystorePath -clcerts -nodes -passin pass:$keystorePassword | openssl x509 -noout -enddate | cut -d= -f2)
  expirationTime=$(python -c "from datetime import datetime; print datetime.strptime('$notAfter', '%b %d %X %Y %Z').strftime('%s')")
  currentTime=`date +%s`
  remainingTime=$(($expirationTime - $currentTime))

  if (( $remainingTime < 1000 )); then
    echo "Your certificate $keystorePath has expired. Please generate a new certificate."
    exit 1
  fi
fi

# discover cluster for store
echo "Find the real d2 cluster for store: $STORE_NAME through in fabric: $FABRIC through d2..."
FIND_CLUSTER_RESULT=`curli --fabric $FABRIC d2://$D2_CLUSTER/discover_cluster/$STORE_NAME`
D2_CLUSTER=`echo $FIND_CLUSTER_RESULT | jq '.d2Service'| tr -d '"'`

echo "Find a Venice router in d2 cluster: $D2_CLUSTER in fabric: $FABRIC through D2..."
FIND_ROUTER_RESULT=`curli --fabric $FABRIC d2://d2Clusters/$D2_CLUSTER`
# choose a router from d2 cluster. Only pick up the https URL.
ROUTER_URL=`echo $FIND_ROUTER_RESULT | jq '[.uris[]| select(.URI|contains("https:"))][0].URI'`

echo ""
echo "Will send a request to $ROUTER_URL, Store $STORE_NAME, Key string: $KEY_STRING..."

java -jar $BASE_DIR/build/libs/venice-thin-client-0.1.jar $STORE_NAME $KEY_STRING $ROUTER_URL $IS_VSON_STORE $SSL_CONFIG
