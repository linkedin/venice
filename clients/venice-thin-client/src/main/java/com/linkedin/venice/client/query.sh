#!/bin/bash

# Venice Thin Client Query Script (with Facet Counting Support)
# Usage: ./query.sh <fabric> <store_name> <key_string> [is_vson_store] [facet_counting_mode] [count_by_value_fields] [count_by_bucket_fields] [top_k] [bucket_definitions]

# Only works on files for now since. It fails for special directories "." and "/" since "basename" and "dirname" returns
# the same value
function real_path() {
  local dir_path=$(cd "$(dirname "$1")" && pwd)
  local file_name=$(basename "$1")
  echo "$dir_path/$file_name"
}

function is_compatible () {
  "$@" >/dev/null 2>&1
}

function is_compatible_date_f() {
    is_compatible date -j -f "%b %d %X %Y %Z" "Jan 1 00:00:00 2022 GMT"
}

function is_compatible_date_d() {
    is_compatible date -d "Jan 1 00:00:00 2022 GMT"
}

is_compatible_date_f
useBsdDate=$?

is_compatible_date_d
useGnuDate=$?

if [ $useBsdDate -eq 1 ] && [ $useGnuDate -eq 1 ] ; then
  echo "ERROR: Cannot identify which flavor of 'date' command to use. Please report the error."
  exit 1
fi

# GNU: date -d "Apr 29 16:43:30 2022 GMT" +"%s"
# BSD: date -j -f "%b %d %X %Y %Z" "Apr 29 16:43:30 2022 GMT" +"%s"
function parse_openssl_date_to_epoch_seconds() {
  if [ $useGnuDate -eq 0 ]; then
    date -d "$1" +"%s"
  elif [ $useBsdDate -eq 0 ]; then
    date -j -f "%b %d %X %Y %Z" "$1" +"%s"
  fi
}

script_real_path=$(real_path "${BASH_SOURCE[0]}") # Absolute path
base_dir=$(dirname "$script_real_path")

function cert_has_enough_time() {
  cert=$1
  password=$2

  not_after=$(2>/dev/null openssl pkcs12 -in "$cert" -clcerts -nodes -passin "pass:$password" | openssl x509 -noout -enddate | cut -d= -f2)
  expiration_time=$(parse_openssl_date_to_epoch_seconds "$not_after")
  current_time=$(date +%s)
  remaining_time=$((expiration_time - current_time))

  # Cert must be valid for a small buffer zone so that during execution of this program the certificate will remain valid.
  if (( remaining_time < 30 )); then    # in seconds
    return 1
  else
    return 0
  fi
}

function download_fat_jar() {
  artifactory_base_path="$1"
  version="$2"
  local_base_path="$3"

  mkdir -p "$local_base_path"
  # Do not use wget, because it is not available on Mac
  curl -s "$artifactory_base_path/$version/venice-thin-client-$version-standalone.jar" > "$local_base_path/venice-thin-client-$version-standalone.tmp"
  # This is to detect interrupted download
  mv "$local_base_path/venice-thin-client-$version-standalone.tmp" "$local_base_path/venice-thin-client-$version-standalone.jar"
}

if [ $# -gt 9 ] || [ $# -lt 3 ]; then
  echo "  Usage: $0 <fabric> <store_name> <key_string> [is_vson_store] [facet_counting_mode] [count_by_value_fields] [count_by_bucket_fields] [top_k] [bucket_definitions]"
  echo "  Example: $0 ei-ltx1 LeapContentRecommendationTest '{\"contractId\":2660,\"memberId\":1,\"modelVersionId\":\"testModel\",\"source\":\"NEARLINE\"}'"
  echo ""
  echo "  Facet counting mode options:"
  echo "    - 'single': Query single key (default)"
  echo "    - 'countByValue': Count distinct values for specified fields"
  echo "    - 'countByBucket': Count records matching bucket predicates"
  echo ""
  echo "  Examples:"
  echo "    # Single key query (original behavior):"
  echo "    $0 ei-ltx1 store_name 'key1'"
  echo ""
  echo "    # Count by value (top 5 most common firstName values):"
  echo "    $0 ei-ltx1 store_name 'key1,key2,key3' false countByValue 'firstName,lastName' 5"
  echo ""
  echo "    # Count by bucket (age ranges):"
  echo "    $0 ei-ltx1 store_name 'key1,key2,key3' false countByBucket 'age' 10 '20-25,26-30,31-35'"
  exit 1
else
  fabric="$1"
  store_name="$2"
  key_string="$3"
  if [ $# -gt 3 ]; then
    is_vson_store="$4"
  else
    is_vson_store="false"
  fi
  # Parse facet counting parameters
  if [ $# -gt 4 ]; then
    facet_counting_mode="$5"
  else
    facet_counting_mode="single"
  fi
  # Normalize facet counting mode to correct case
  if [[ "$facet_counting_mode" == "countbyvalue" ]]; then
    facet_counting_mode="countByValue"
  elif [[ "$facet_counting_mode" == "countbybucket" ]]; then
    facet_counting_mode="countByBucket"
  fi
  if [ $# -gt 5 ]; then
    count_by_value_fields="$6"
  else
    count_by_value_fields=""
  fi
  if [ $# -gt 6 ]; then
    count_by_bucket_fields="$7"
  else
    count_by_bucket_fields=""
  fi
  if [ $# -gt 7 ]; then
    top_k="$8"
  else
    top_k="10"
  fi
  if [ $# -gt 8 ]; then
    bucket_definitions="$9"
  else
    bucket_definitions=""
  fi
  # For countByBucket mode, if we have 6 arguments, the 6th is the field name and the 7th is the bucket definitions
  if [[ "$facet_counting_mode" == "countByBucket" && $# -eq 7 ]]; then
    count_by_bucket_fields="$6"
    bucket_definitions="$7"
  fi
  # For countByValue mode, if we have 7 arguments, the 6th is the field name and the 7th is the top_k value
  if [[ "$facet_counting_mode" == "countByValue" && $# -eq 7 ]]; then
    count_by_value_fields="$6"
    top_k="$7"
  fi
fi

# Prepare Venice client jar
artifactory_base_path="https://artifactory.corp.linkedin.com:8083/artifactory/DDS/com/linkedin/venice-thin-client/venice-thin-client"
local_base_path="build/venice-thin-client/libs"
latest_version=$(2>/dev/null curl -s "$artifactory_base_path/" | grep "^<a href=" | cut -d'"' -f2 | sort -V | tail -n1 | tr -d '/')

if [[ ! -e "$local_base_path" ]]; then
  echo "First time running the script. This will take a short while, but next time you won't have to wait again."
  if [[ -n "$latest_version" ]]; then
    echo "Downloading Venice client v$latest_version ..."
    download_fat_jar "$artifactory_base_path" "$latest_version" "$local_base_path"
  else
    echo "WARN: Cannot download Venice client fat jar from Artifactory. Try to build it instead ..."
    mint build
  fi
fi

# Check again
jar_file=$(find "$local_base_path" -type f -name 'venice-thin-client-*-standalone.jar' | sort -V | tail -1)
if [[ ! -e "$jar_file" ]]; then
  echo "ERROR: Cannot locate Venice client fat jar. Please report the error."
  exit 1
fi

# Prepare SSL configuration file
base_dir=$(pwd)
ssl_config_file="${base_dir}/ssl.config"
ssl_configs="ssl.enabled=true
ssl.keystore.type=PKCS12
ssl.keystore.password=work_around_jdk-6879539
ssl.keystore.location=${base_dir}/identity.p12
ssl.truststore.password=changeit
ssl.truststore.location=/etc/riddler/cacerts"
echo -n "$ssl_configs" > "$ssl_config_file"

# Validate certificate
if ! grep -q "ssl.keystore.password=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.password"
  exit 1
elif ! grep -q "ssl.keystore.location=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.location"
  exit 1
elif ! grep -q "ssl.keystore.type=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.type"
  exit 1
elif ! grep -q "ssl.truststore.location=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.truststore.location"
  exit 1
elif ! grep -q "ssl.truststore.password=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.truststore.password"
  exit 1
elif ! grep -q "ssl.enabled=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.enabled"
  exit 1
fi

keystore_password=$(grep "ssl.keystore.password=" "$ssl_config_file" | cut -d= -f2-)
keystore_path=$(grep "ssl.keystore.location=" "$ssl_config_file" | cut -d= -f2-)
keystore_real_path=$(realpath "$keystore_path")
truststore_path=$(grep "ssl.truststore.location=" "$ssl_config_file" | cut -d= -f2-)

if [[ "$keystore_path" != "$keystore_real_path" ]]; then
  echo "ERROR: Please use absolute path for $keystore_path"
  exit 1
elif [[ ! -e "$truststore_path" ]]; then
  echo "ERROR: $truststore_path does not exist"
  exit 1
elif [[ ! -e "$keystore_path" ]]; then
  echo
  echo "Creating certificate ..."
  if ! id-tool grestin sign -o "$base_dir"; then
    echo "ERROR: Failed to create certificate"
    exit 1
  fi
elif ! cert_has_enough_time "$keystore_path" "$keystore_password"; then
  echo "Your certificate $keystore_path has expired. Creating new certificate ..."
  if ! id-tool grestin sign -o "$base_dir"; then
    echo "ERROR: Failed to renew certificate"
    exit 1
  fi
fi

if [[ ! -e "${base_dir}/identity.p12" ]]; then
  echo "ERROR: Cannot locate identity.p12 keystore file. Please report the error."
  exit 1
elif ! cert_has_enough_time "$keystore_path" "$keystore_password"; then
  echo "ERROR: Cannot renew expired certificate. Please report the error."
  exit 1
fi

echo "Gathering information from remote ..."

# Discover cluster for store
d2_result=$(2>.stderr curli --no-log --force-insecure-d2 --fabric "$fabric" "d2://venice-discovery/discover_cluster/$store_name")
error=$(<.stderr)

if [[ "$error" == *"[ERROR]"* ]]; then
  echo "$error"
  exit 1
fi

if [[ "$d2_result" == *"doesn't exist"* ]]; then
  echo "ERROR: Invalid store name $store_name"
  exit 1
elif [[ -z "$d2_result" ]]; then
  echo "ERROR: D2 returned nothing"
  exit 1
fi

d2_cluster=$(echo "$d2_result" | grep "d2Service" | cut -d: -f2 | tr -d ' ",')

if [[ -z "$d2_cluster" ]]; then
  echo "ERROR: Cannot determine D2 cluster. Please try again. If the issue persists, please report the error to Venice team."
  echo "D2 result: $d2_result"
  exit 1
fi

# Choose a Venice router that supports https
router_url=$(2>/dev/null curli --no-log --force-insecure-d2 --fabric "$fabric" "d2://d2Clusters/$d2_cluster" | grep "https://" | head -1 | cut -d: -f2- | tr -d ' ",')

if [[ -z $router_url ]]; then
  echo "ERROR: Cannot determine router URL. Please try again. If the issue persists, please report the error to Venice team."
  exit 1
fi

echo "Checking local environment ..."

echo

# Prepare for invocation
echo "Will send a request to $router_url for store $store_name with key string: $key_string"

router_hostname_and_port=$(echo "$router_url" | sed 's/https:\/\///')
router_hostname=$(echo "$router_hostname_and_port" | cut -d: -f1)
router_port=$(echo "$router_hostname_and_port" | cut -d: -f2)

# Skip connection test for IPv6 addresses as they may not be supported in local environment
if [[ "$router_hostname" =~ ^\[.*\]$ ]]; then
  echo "Skipping connection test for IPv6 address: $router_hostname"
else
  echo "Skipping connection test for router: $router_hostname"
  # if ! echo > "/dev/tcp/$router_hostname/$router_port"; then
  #   echo
  #   echo "ERROR: Failed to establish connection to Venice router $router_hostname"
  #   echo "You must run this tool in $fabric"
  #   exit 1
  # fi
fi

# Build parameters
jar_args="$store_name $key_string $router_url $is_vson_store $ssl_config_file"
if [[ "$facet_counting_mode" != "single" ]]; then
  jar_args="$jar_args $facet_counting_mode"
  if [[ "$facet_counting_mode" == "countByValue" ]]; then
    jar_args="$jar_args $count_by_value_fields $top_k"
  elif [[ "$facet_counting_mode" == "countByBucket" ]]; then
    jar_args="$jar_args $count_by_bucket_fields $bucket_definitions"
  fi
fi

echo "Debug: facet_counting_mode=$facet_counting_mode, count_by_value_fields=$count_by_value_fields, top_k=$top_k"
echo "Debug: jar_args=$jar_args"

# Use locally compiled classes if available
project_root="/Users/leoli/Desktop/venice-by-leo-2"
build_classes_dir="$project_root/clients/venice-thin-client/build/classes/java/main"
build_resources_dir="$project_root/clients/venice-thin-client/build/resources/main"
if [[ -d "$build_classes_dir" ]]; then
  classpath="$build_classes_dir:$build_resources_dir"
  for jar in $(find "$project_root" -name "*.jar" -type f | head -20); do
    classpath="$classpath:$jar"
  done
  java -cp "$classpath" com.linkedin.venice.client.store.QueryTool $jar_args 2>.stderr
else
  java -jar "$jar_file" $jar_args 2>.stderr
fi
query_command_status=$?
error_message=$(<.stderr)
echo "$error_message"
if [ $query_command_status -ne 0 ] && [[ $error_message = *"Error: Invalid or corrupt jarfile"* ]]; then
  installed_java_version="$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')"
  echo "Currently configured JRE version ($installed_java_version) may be outdated. Please update JAVA_HOME and try again."
fi
