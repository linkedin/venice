#!/bin/bash

DEFAULT_AVRO_TOOLS_JAR=`find ~/.gradle/caches/ | grep 'avro-tools-1.4.0.jar' | head -n 1`

AVRO_SCHEMAS_PATH="venice-common/src/main/resources/avro/*/*"
CODE_GEN_PATH="venice-common/src/main/java"
FULL_CODE_GEN_PATH="$CODE_GEN_PATH/com/linkedin/venice/kafka/protocol/*.java"

if [[ $# < 1 ]]; then
  echo "Usage: $0 avro_tools_path"
  echo ""
  echo "    avro_tools_path: full path to the avro-tools-1.4.0.jar file (required). If you use 'default', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_JAR"
  echo ""
  echo "The $0 script uses avro-tools to generate SpecificRecord classes for the Avro schemas stored in:"
  echo ""
  echo "    $AVRO_SCHEMAS_PATH"
  echo ""
  echo "The auto-generated classes are purged before each run and then re-generated here:"
  echo ""
  echo "    $FULL_CODE_GEN_PATH"
  echo ""
  exit 1
fi

AVRO_TOOLS_PATH_PARAM=$1

if [[ $AVRO_TOOLS_PATH_PARAM -eq 'default' ]]; then
  AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_JAR
else
  AVRO_TOOLS_JAR=$AVRO_TOOLS_PATH_PARAM
fi

echo "Using AVRO_TOOLS_JAR=$AVRO_TOOLS_JAR"

rm $FULL_CODE_GEN_PATH

echo "Finished deleting old files. About to generate new ones..."

java -jar $AVRO_TOOLS_JAR compile schema $AVRO_SCHEMAS_PATH $CODE_GEN_PATH

echo "Done!"
