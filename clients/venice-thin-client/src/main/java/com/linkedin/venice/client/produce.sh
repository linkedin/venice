#!/bin/bash

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
  product="$4"
  suffix="$5"

  mkdir -p "$local_base_path"
  # Do not use wget, because it is not available on Mac
  curl -s "$artifactory_base_path/$version/$product-$version$suffix.jar" > "$local_base_path/$product-$version$suffix.tmp"
  # This is to detect interrupted download
  mv "$local_base_path/$product-$version$suffix.tmp" "$local_base_path/$product-$version$suffix.jar"
}

if [ $# -ne 4 ]; then
  example_value_string='{"firstName": "Venice", "lastName": "McVeniceFace", "age": 30}'
  echo "  Usage: $0 <fabric> <store_name> <key_string> <value_string>"
  echo "  Example: $0 ei-ltx1 your-venice-store 'key_101' '$example_value_string'"
  exit 1
else
  # Initialize essential variables
  fabric="$1"
  store_name="$2"
  key_string="$3"
  value_string="$4"
fi

case "$fabric" in
  ei-ltx1)
    d2_zk_host="zk-ltx1-d2.stg.linkedin.com:12913"
    ;;

  ei4)
    d2_zk_host="zk-ei4-d2.int.linkedin.com:12913"
    ;;

  prod-lva1)
    d2_zk_host="zk-lva1-shared.prod.linkedin.com:12913"
    ;;

  prod-ltx1)
      d2_zk_host="zk-ltx1-shared.prod.linkedin.com:12913"
      ;;

  prod-lor1)
      d2_zk_host="zk-lor1-d2.prod.linkedin.com:12913"
      ;;

  *)
    echo "Unknown fabric '$fabric'"
    exit 1
    ;;
esac


function prepare_jar_file() {
  local_base_path="$1"
  artifactory_base_path="$2"
  product_name="$3"
  jar_suffix="$4"

  # Prepare Venice producer jar
  latest_version=$(2>/dev/null curl -s "$artifactory_base_path/" | grep "^<a href=" | cut -d'"' -f2 | sort -V | tail -n1 | tr -d '/')

  if [[ ! -e "$local_base_path" ]]; then
    echo "First time running the script. This will take a short while, but next time you won't have to wait again."

    # Download the fat jar if possible
    if [[ -n "$latest_version" ]]; then
      echo "Downloading Venice producer v$latest_version ..."
      download_fat_jar "$artifactory_base_path" "$latest_version" "$local_base_path" "$product_name" "$jar_suffix"
    else
      echo "ERROR: Cannot download $product_name jar from Artifactory"
      exit 1
    fi
  fi

  jar_file=$(find "$local_base_path" -type f -name "$product_name-*$jar_suffix.jar" | sort -V | tail -1)
  echo "Found jar file for $product_name: $jar_file"

  # Auto upgrade local jar to the latest available version
  current_version=$(echo "$jar_file" | grep -Eo "([0-9]+\.?){3}")
  # It's possible that local version is actually newer than the latest on Artifactory, in that case we don't want to download
  newer_version=$(echo -e "$current_version\n$latest_version" | sort -V | tail -1)
  if [[ "$current_version" != "$newer_version" ]]; then
    echo "Updating $product_name from v$current_version to v$latest_version ..."
    download_fat_jar "$artifactory_base_path" "$latest_version" "$local_base_path" "$product_name" "$jar_suffix"
    rm "$jar_file"    # remove old version
  fi
}

# Prepare jar file for Venice Producer
local_base_path="build/venice-producer/libs"
artifactory_base_path="https://artifactory.corp.linkedin.com:8083/artifactory/all/com/linkedin/venice/venice-producer"
product_name="venice-producer"
jar_suffix="-all"
prepare_jar_file "$local_base_path" "$artifactory_base_path" "$product_name" "$jar_suffix"

# Prepare jar file for Venice Custom Partitioner
artifactory_base_path_for_custom_partitioner="https://artifactory.corp.linkedin.com:8083/artifactory/all/com/linkedin/livenice/venice-mp-common"
product_name_for_custom_partitioner="venice-mp-common"
jar_suffix_for_custom_partitioner=""
prepare_jar_file "$local_base_path" "$artifactory_base_path_for_custom_partitioner" "$product_name_for_custom_partitioner" "$jar_suffix_for_custom_partitioner"

# Double check jar files
jar_file=$(find "$local_base_path" -type f -name 'venice-producer-*-all.jar' | sort -V | tail -1)
if [[ ! -e "$jar_file" ]]; then
  echo "ERROR: Cannot locate Venice producer fat jar. Please report the error."
  exit 1
fi

partitioner_jar_file=$(find "$local_base_path" -type f -name 'venice-mp-common-*.jar' | sort -V | tail -1)
if [[ ! -e "$partitioner_jar_file" ]]; then
  echo "ERROR: Cannot locate Venice partitioner jar. Please report the error."
  exit 1
fi

keystore_password="work_around_jdk-6879539"
keystore_path="${base_dir}/identity.p12"
keystore_real_path=$(real_path "$keystore_path")
truststore_path="/etc/riddler/cacerts"

# Prepare configuration file
ssl_configs="ssl.enabled=true
ssl.keystore.type=PKCS12
ssl.keystore.password=$keystore_password
ssl.keystore.location=$keystore_path
ssl.truststore.type=JKS
ssl.truststore.password=changeit
ssl.truststore.location=$truststore_path
ssl.key.password=work_around_jdk-6879539
ssl.keymanager.algorithm=SunX509
ssl.trustmanager.algorithm=SunX509
ssl.secure.random.implementation=SHA1PRNG
security.protocol=SSL"

config_file="${base_dir}/online_producer.config"
rm -f "$config_file"
touch "$config_file"

echo -n "$ssl_configs" >> "$config_file"

if [[ "$keystore_path" != "$keystore_real_path" ]]; then
  echo "ERROR: Please use absolute path for $keystore_path"
  exit 1
elif [[ ! -e "$truststore_path" ]]; then
  echo "ERROR: $truststore_path does not exist"
  exit 1
elif [[ ! -e "$keystore_path" ]]; then
  # $keystore_path does not exist
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

if [[ ! -e "$keystore_path" ]]; then
  echo "ERROR: Cannot locate identity.p12 keystore file. Please report the error."
  exit 1
elif ! cert_has_enough_time "$keystore_path" "$keystore_password"; then
  echo "ERROR: Cannot renew expired certificate. Please report the error."
  exit 1
fi

echo "Checking local environment ..."

# Prepare for invocation
# Check if java exists
if ! command -v java > /dev/null; then
  # Cannot find java in default $PATH. Give it a second chance.
  # The following works on Linux only.
  jdk_dir=$(find '/export/apps/jdk/' -type d -name 'JDK-*' | sort -V | tail -1) # Latest compatible version
  if [[ -z $jdk_dir ]]; then
    # Most likely running on a Mac.
    echo 'ERROR: Cannot find "java" from PATH'
    exit 1
  fi
  PATH="$jdk_dir/bin:$PATH"
fi

echo
echo "Will write using D2 Zk $d2_zk_host for store $store_name with key string: $key_string and value string: $value_string"

main_class="com.linkedin.venice.producer.online.ProducerTool"
java -cp "$jar_file:$partitioner_jar_file" "$main_class" -s "$store_name" -k "$key_string" -v "$value_string" -vu "$d2_zk_host" -cp "$config_file" 2>.stderr
query_command_status=$?
error_message=$(<.stderr)
echo "$error_message"
if [ $query_command_status -ne 0 ] && [[ $error_message = *"Error: Invalid or corrupt jarfile"* ]]; then
  installed_java_version="$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')"
  echo "Currently configured JRE version ($installed_java_version) may be outdated. Please update JAVA_HOME and try again."
fi