package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.NotNull;


/**
 * Functional interface for converting PubSub messages to Spark InternalRows.
 */
@FunctionalInterface
public interface PubSubMessageConverter {
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
  InternalRow convert(@NotNull DefaultPubSubMessage pubSubMessage, String region, int partitionNumber, long offset);
}
