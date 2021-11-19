package com.linkedin.davinci.replication;

import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class to store Replication Metadata ByteBuffer and the value schema id used to generate the schema for it.
 */
public class ReplicationMetadataWithValueSchemaId {
  private final int valueSchemaId;
  private GenericRecord replicationMetadataRecord;

  public ReplicationMetadataWithValueSchemaId(int valueSchemaId, GenericRecord replicationMetadataRecord) {
    this.valueSchemaId = valueSchemaId;
    this.replicationMetadataRecord = replicationMetadataRecord;
  }

  public GenericRecord getReplicationMetadataRecord() {
    return replicationMetadataRecord;
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
  public static ReplicationMetadataWithValueSchemaId convertStorageEngineBytes(byte[] rawBytes, MergeConflictResolver mergeConflictResolver) {
    if (rawBytes == null) {
      return null;
    }
    ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(rawBytes);
    final int valueSchemaId = replicationMetadataWithValueSchema.getInt();

    return new ReplicationMetadataWithValueSchemaId(valueSchemaId,
        mergeConflictResolver.getReplicationMetadataRecordFromByteBuffer(replicationMetadataWithValueSchema, valueSchemaId));
  }
}
