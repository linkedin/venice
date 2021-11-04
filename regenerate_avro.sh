#!/bin/bash

DEFAULT_AVRO_TOOLS_DIR_NAME="avro_tools"
DEFAULT_AVRO_TOOLS_JAR_VERSION="1.4.0"
DEFAULT_AVRO_TOOLS_JAR_NAME="avro-tools-$DEFAULT_AVRO_TOOLS_JAR_VERSION.jar"
DEFAULT_AVRO_TOOLS_JAR="$DEFAULT_AVRO_TOOLS_DIR_NAME/$DEFAULT_AVRO_TOOLS_JAR_NAME"

mkdir -p "$DEFAULT_AVRO_TOOLS_DIR_NAME"

if [ ! -f "$DEFAULT_AVRO_TOOLS_JAR" ]; then
  wget "https://artifactory.corp.linkedin.com:8083/artifactory/ext-libraries/org/apache/avro/avro-tools/$DEFAULT_AVRO_TOOLS_JAR_VERSION/avro-tools-$DEFAULT_AVRO_TOOLS_JAR_VERSION.jar" -P "$DEFAULT_AVRO_TOOLS_DIR_NAME" --no-check-certificate
fi

AVRO_SCHEMAS_PATH=(
  "venice-common/src/main/resources/avro/KafkaMessageEnvelope/v10/*"
  "venice-common/src/main/resources/avro/PushJobStatusRecord/PushJobStatusRecordKey/v1/*"
  "venice-common/src/main/resources/avro/PartitionState/v10/*"
  "venice-common/src/main/resources/avro/StoreVersionState/v7/*"
  "venice-common/src/main/resources/avro/ChunkedValueManifest/v-20/*"
  "venice-common/src/main/resources/avro/ChunkedKeySuffix/*"
  "venice-common/src/main/resources/avro/ParticipantMessage/ParticipantMessageKey/*"
  "venice-common/src/main/resources/avro/ParticipantMessage/ParticipantMessageValue/v1/*"
  "venice-controller/src/main/resources/avro/AdminOperation/v57/*"
  "venice-schema-common/src/main/resources/avro/MultiGetResponseRecord/*"
  "venice-schema-common/src/main/resources/avro/MultiGetClientRequestKey/*"
  "venice-schema-common/src/main/resources/avro/MultiGetRouterRequestKey/*"
  "venice-schema-common/src/main/resources/avro/ComputeRequest/v1/*"
  "venice-schema-common/src/main/resources/avro/ComputeRequest/v2/*"
  "venice-schema-common/src/main/resources/avro/ComputeRequest/v3/*"
  "venice-schema-common/src/main/resources/avro/ComputeRequest/v4/*"
  "venice-schema-common/src/main/resources/avro/ComputeResponseRecord/*"
  "venice-schema-common/src/main/resources/avro/ComputeRouterRequestKey/*"
  "venice-schema-common/src/main/resources/avro/StreamingFooterRecord/*"
  "venice-common/src/main/resources/avro/PushJobDetails/v3/*"
  "venice-common/src/main/resources/avro/BatchJobHeartbeatKey/v1/*"
  "venice-common/src/main/resources/avro/BatchJobHeartbeatValue/v1/*"
  "venice-common/src/main/resources/avro/StoreMetadata/StoreMetadataKey/*"
  "venice-common/src/main/resources/avro/StoreMetadata/StoreMetadataValue/v2/*"
  "venice-common/src/main/resources/avro/StoreMeta/StoreMetaKey/*"
  "venice-common/src/main/resources/avro/StoreMeta/StoreMetaValue/v7/*"
  "venice-common/src/main/resources/avro/PushStatus/PushStatusKey/v1/*"
  "venice-common/src/main/resources/avro/PushStatus/PushStatusValue/v1/*"
  "venice-common/src/main/resources/avro/AdminResponseRecord/v1/*"
  "hadoop-to-venice-bridge/src/main/resources/avro/KafkaInputMapperValue/*"
  "venice-common/src/main/resources/avro/PushStatus/PushStatusValueWriteOpRecord/v1/*"
)
CODE_GEN_PATH=(
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-controller/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-schema-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "venice-common/src/main/java"
  "hadoop-to-venice-bridge/src/main/java"
  "venice-common/src/main/java"
)
FULL_CODE_GEN_PATH=(
  "${CODE_GEN_PATH[0]}/com/linkedin/venice/kafka/protocol/*.java"
  "${CODE_GEN_PATH[1]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[2]}/com/linkedin/venice/kafka/protocol/state/*.java"
  "${CODE_GEN_PATH[3]}/com/linkedin/venice/kafka/protocol/state/*.java"
  "${CODE_GEN_PATH[4]}/com/linkedin/venice/storage/protocol/*.java"
  "${CODE_GEN_PATH[5]}/com/linkedin/venice/storage/protocol/*.java"
  "${CODE_GEN_PATH[6]}/com/linkedin/venice/participant/protocol/*.java"
  "${CODE_GEN_PATH[7]}/com/linkedin/venice/participant/protocol/*.java"
  "${CODE_GEN_PATH[8]}/com/linkedin/venice/controller/kafka/protocol/admin/*.java"
  "${CODE_GEN_PATH[9]}/com/linkedin/venice/read/protocol/response/*.java"
  "${CODE_GEN_PATH[10]}/com/linkedin/venice/read/protocol/request/client/*.java"
  "${CODE_GEN_PATH[11]}/com/linkedin/venice/read/protocol/request/router/*.java"
  "${CODE_GEN_PATH[12]}/com/linkedin/venice/compute/protocol/request/*.java"
  "${CODE_GEN_PATH[13]}/com/linkedin/venice/compute/protocol/request/*.java"
  "${CODE_GEN_PATH[14]}/com/linkedin/venice/compute/protocol/request/*.java"
  "${CODE_GEN_PATH[15]}/com/linkedin/venice/compute/protocol/request/*.java"
  "${CODE_GEN_PATH[16]}/com/linkedin/venice/compute/protocol/response/*.java"
  "${CODE_GEN_PATH[17]}/com/linkedin/venice/compute/protocol/request/router/*.java"
  "${CODE_GEN_PATH[18]}/com/linkedin/venice/read/protocol/response/streaming/*.java"
  "${CODE_GEN_PATH[19]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[20]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[21]}/com/linkedin/venice/status/protocol/*.java"
  "${CODE_GEN_PATH[22]}/com/linkedin/venice/meta/systemstore/schemas/*.java"
  "${CODE_GEN_PATH[23]}/com/linkedin/venice/meta/systemstore/schemas/*.java"
  "${CODE_GEN_PATH[24]}/com/linkedin/venice/systemstore/schemas/*.java"
  "${CODE_GEN_PATH[25]}/com/linkedin/venice/systemstore/schemas/*.java"
  "${CODE_GEN_PATH[26]}/com/linkedin/venice/pushstatus/*.java"
  "${CODE_GEN_PATH[27]}/com/linkedin/venice/pushstatus/*.java"
  "${CODE_GEN_PATH[28]}/com/linkedin/venice/admin/protocol/response/*.java"
  "${CODE_GEN_PATH[29]}/com/linkedin/venice/hadoop/input/kafka/avro/*.java"
  "${CODE_GEN_PATH[30]}/com/linkedin/venice/pushstatus/*.java"
)

if [[ $1 = "-h" ]]; then
  echo "Usage: $0 [avro_tools_path]"
  echo ""
  echo "    avro_tools_path: full path to the avro-tools jar file. If you don't specify a value or use 'default', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_JAR"
  echo ""
  echo "The $0 script uses avro-tools to generate SpecificRecord classes for the Avro schemas stored in:"
  echo ""
  for path in ${AVRO_SCHEMAS_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  echo "The auto-generated classes are purged before each run and then re-generated here:"
  echo ""
  for path in ${FULL_CODE_GEN_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  exit 1
fi

AVRO_TOOLS_PATH_PARAM=$1

if [[ -z "$AVRO_TOOLS_PATH_PARAM" || "$AVRO_TOOLS_PATH_PARAM" = 'default' ]]; then
  AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_JAR
else
  AVRO_TOOLS_JAR=$AVRO_TOOLS_PATH_PARAM
fi

if [ ! -f "$AVRO_TOOLS_JAR" ]; then
  echo "$AVRO_TOOLS_JAR doesn't exist. Exiting."
  exit 1
else
  echo "Using AVRO_TOOLS_JAR=$AVRO_TOOLS_JAR"
fi

for path in ${FULL_CODE_GEN_PATH[@]}; do
  rm $path
done

echo "Finished deleting old files. About to generate new ones..."

for (( i=0; i<${#FULL_CODE_GEN_PATH[@]}; i++ )); do
  java -jar "$AVRO_TOOLS_JAR" compile schema ${AVRO_SCHEMAS_PATH[i]} ${CODE_GEN_PATH[i]}
done

# Don't generate class: com.linkedin.venice.kafka.protocol.GUID.java since it contains changes required to support Avro-1.8
git checkout -- venice-common/src/main/java/com/linkedin/venice/kafka/protocol/GUID.java
echo "Done!"
