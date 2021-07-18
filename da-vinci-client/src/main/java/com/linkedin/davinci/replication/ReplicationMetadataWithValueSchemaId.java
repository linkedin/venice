package com.linkedin.davinci.replication;

import java.nio.ByteBuffer;


/**
 * A POJO class to store Replication Metadata ByteBuffer and the value schema id used to generate the schema for it.
 */
public class ReplicationMetadataWithValueSchemaId {
  private ByteBuffer value;
  private int valueSchemaId;

  public ReplicationMetadataWithValueSchemaId(ByteBuffer value, int valueSchemaId) {
    this.value = value;
    this.valueSchemaId = valueSchemaId;
  }

  public ByteBuffer getReplicationMetadata() {
    return value;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  /**
   * The Storage Engine stores the value schema id as a 4 byte header before the raw bytes of the replication metadata.
   * This function is a utility to extract the value schema id and the raw bytes into a {@link ReplicationMetadataWithValueSchemaId}
   * object.
   * @param rawBytes The raw bytes obtained from the storage engine.
   * @return A {@link ReplicationMetadataWithValueSchemaId} object composed by extracting the value schema id from the
   * header of the replication metadata stored in RMD column family.
   */
  public static ReplicationMetadataWithValueSchemaId getFromStorageEngineBytes(byte[] rawBytes) {
    if (rawBytes == null) {
      return null;
    }

    ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(rawBytes);
    int valueSchemaId = replicationMetadataWithValueSchema.getInt();
    return new ReplicationMetadataWithValueSchemaId(replicationMetadataWithValueSchema, valueSchemaId);
  }
}
