The avsc files in this directory are used by the `regenerate_avro.sh` script at the root of the Venice repo in order to auto-generated the Java classes located at:

venice-common/src/main/java/com/linkedin/venice/kafka/protocol/*.java

These files should be treated as immutable. Once a protocol version is created and used in any environment (especially prod), then that version should not change. New versions of the protocol can be created in order to handle graceful evolution.
