package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.jetbrains.annotations.NotNull;


/**
 * Converts a PubSub message to a Spark InternalRow.
 * it preserves the schema, replication metadata, and other necessary fields
 */
public class VenicePubSubMessageToRow implements PubSubMessageConverter {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
  private static final int NON_EXISTING_SCHEMA_ID = Integer.MAX_VALUE;

  /**
   * Static factory method to maintain backward compatibility.
   */
  public static InternalRow convertPubSubMessageToRow(
      @NotNull DefaultPubSubMessage pubSubMessage,
      String region,
      int partitionNumber,
      long offset) {
    return new VenicePubSubMessageToRow().convert(pubSubMessage, region, partitionNumber, offset);
  }

  /**
   * Converts a PubSub message to a Spark InternalRow.
   *
   * @param pubSubMessage The PubSub message to process. Contains key, value, and metadata.
   * @param region The region identifier to include in the row.
   * @param partitionNumber The partition number to include in the row.
   * @return An InternalRow containing the processed message data.
   *         The row includes the following fields:
   *         1. Region (String)
   *         2. Partition number (int)
   *         3. Message type (int)
   *         4. Offset (long)
   *         5. Schema ID (int)
   *         6. Key bytes (byte[])
   *         7. Value bytes (byte[])
   *         8. Replication metadata payload bytes (byte[])
   *         9. Replication metadata version ID (int)
   *         See {@link com.linkedin.venice.spark.SparkConstants#RAW_PUBSUB_INPUT_TABLE_SCHEMA} for the schema definition.
   */
  @Override
  public InternalRow convert(
      @NotNull DefaultPubSubMessage pubSubMessage,
      String region,
      int partitionNumber,
      long offset) {

    KafkaKey pubSubMessageKey = pubSubMessage.getKey();
    KafkaMessageEnvelope pubSubMessageValue = pubSubMessage.getValue();
    MessageType pubSubMessageType = MessageType.valueOf(pubSubMessageValue);

    // Spark row setup :
    ByteBuffer key = ByteBuffer.wrap(pubSubMessageKey.getKey(), 0, pubSubMessageKey.getKeyLength());
    ByteBuffer value;
    int messageType;
    int schemaId;
    ByteBuffer replicationMetadataPayload;
    int replicationMetadataVersionId;

    switch (pubSubMessageType) {
      case PUT:
        Put put = (Put) pubSubMessageValue.payloadUnion;
        messageType = MessageType.PUT.getValue();
        value = put.putValue;
        schemaId = put.schemaId; // chunking will be handled down the road in spark job.
        replicationMetadataPayload = put.replicationMetadataPayload;
        replicationMetadataVersionId = put.replicationMetadataVersionId;
        break;
      case DELETE:
        messageType = MessageType.DELETE.getValue();
        Delete delete = (Delete) pubSubMessageValue.payloadUnion;
        schemaId = delete.schemaId;
        value = EMPTY_BYTE_BUFFER;

        replicationMetadataPayload = delete.replicationMetadataPayload;
        replicationMetadataVersionId = delete.replicationMetadataVersionId;
        break;
      default:
        messageType = -1; // this is an error condition
        schemaId = NON_EXISTING_SCHEMA_ID;
        value = EMPTY_BYTE_BUFFER;
        replicationMetadataPayload = EMPTY_BYTE_BUFFER;
        replicationMetadataVersionId = Integer.MAX_VALUE;
        // we don't care about messages other than PUT and DELETE
    }

    /**
     *  See {@link com.linkedin.venice.spark.SparkConstants#RAW_PUBSUB_INPUT_TABLE_SCHEMA} for the schema definition.
     */
    return new GenericInternalRow(
        new Object[] { region, partitionNumber, messageType, offset, schemaId, ByteUtils.extractByteArray(key),
            ByteUtils.extractByteArray(value), ByteUtils.extractByteArray(replicationMetadataPayload),
            replicationMetadataVersionId });
  }
}
