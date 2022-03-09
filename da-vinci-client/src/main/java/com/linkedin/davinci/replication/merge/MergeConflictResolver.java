package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.VeniceConstants.*;

/**
 * TODO schema validation of old and new schema for WC enabled stores.
 * The workflow is
 * Query old replication metadata. If it's null (and running in first batch push merge policy), then write the new value directly.
 * If the old replication metadata exists, then deserialize it and run Merge<BB>.
 * If the incoming TS is higher than the entirety of the old replication metadata, then write the new value directly.
 * If the incoming TS is lower than the entirety of the old replication metadata, then drop the new value.
 * If the incoming TS is partially higher, partially lower, than the old replication metadata, then query the old value, deserialize it, and pass it to Merge<GR>, Merge<Map> or Merge<List> .
 */
public class MergeConflictResolver {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepository;
  private final Function<Integer, GenericRecord> newRmdCreator;
  private final MergeGenericRecord mergeGenericRecord;
  private final MergeByteBuffer mergeByteBuffer;

  public MergeConflictResolver(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      Function<Integer, GenericRecord> newRmdCreator
  ) {
    this.schemaRepository = schemaRepository;
    this.storeName = storeName;
    this.newRmdCreator = newRmdCreator;
    this.mergeGenericRecord = null; // TODO: use it to handle per-field timestamp cases.
    this.mergeByteBuffer = new MergeByteBuffer();
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldValue A Lazy supplier of currently persisted value.
   * @param rmdWithValueSchemaID The replication metadata of the currently persisted value and the value schema ID.
   * @param newValue The value in the incoming record.
   * @param putOperationTimestamp The logical timestamp of the incoming record.
   * @param newValueSchemaID The schema id of the value in the incoming record.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                               to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueColoID ID of the colo/fabric where this new Put request came from.
   *
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult put(
      Lazy<ByteBuffer> oldValue,
      Optional<ReplicationMetadataWithValueSchemaId> rmdWithValueSchemaID,
      ByteBuffer newValue,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      final int newValueColoID
  ) {
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (!rmdWithValueSchemaID.isPresent()) {
      /**
       * Replication metadata could be null in two cases:
       *    1. There is no value corresponding to the key
       *    2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
       *
       * Specifies that no per-record replication metadata should be persisted for batch push data.
       * In such cases, the incoming PUT operation will be applied directly and we should store the updated RMD for it.
       */
      GenericRecord newReplicationMetadata = newRmdCreator.apply(newValueSchemaID);
      newReplicationMetadata.put(TIMESTAMP_FIELD_NAME, putOperationTimestamp);
      // A record which didn't come from an RT topic or has null metadata should have no offset vector.
      newReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
          MergeUtils.mergeOffsetVectors(Optional.empty(), newValueSourceOffset, newValueSourceBrokerID));

      return new MergeConflictResult(Optional.of(newValue), newValueSchemaID, true, newReplicationMetadata);

    } else if (rmdWithValueSchemaID.get().getValueSchemaId() <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = "
          + storeName + "; schema ID = " + rmdWithValueSchemaID.get().getValueSchemaId());
    }

    final GenericRecord oldReplicationMetadataRecord = rmdWithValueSchemaID.get().getReplicationMetadataRecord();
    Object existingTimestampObject = oldReplicationMetadataRecord.get(TIMESTAMP_FIELD_NAME);
    Merge.RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(existingTimestampObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        ValueAndReplicationMetadata<ByteBuffer> valueAndRmd = new ValueAndReplicationMetadata<>(oldValue, oldReplicationMetadataRecord);
        valueAndRmd = mergeByteBuffer.put(
            valueAndRmd,
            newValue,
            putOperationTimestamp,
            newValueColoID,
            newValueSourceOffset,
            newValueSourceBrokerID
        );
        if (valueAndRmd.isUpdateIgnored()) {
          return MergeConflictResult.getIgnoredResult();
        } else {
          return new MergeConflictResult(Optional.ofNullable(valueAndRmd.getValue()), newValueSchemaID, true, valueAndRmd.getReplicationMetadata());
        }

      case PER_FIELD_TIMESTAMP:
        // TODO: Handle conflict resolution for write-compute.
        throw new VeniceUnsupportedOperationException("Field level replication metadata not supported");

      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + rmdTimestampType);
    }
  }

  /**
   * Perform conflict resolution when the incoming operation is a DELETE operation.
   *
   * @param rmdWithValueSchemaID The replication metadata of the currently persisted value and the value schema ID.
   * @param deleteOperationTimestamp The logical timestamp of the incoming record.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult delete(
      Optional<ReplicationMetadataWithValueSchemaId> rmdWithValueSchemaID,
      long deleteOperationTimestamp,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    /**
     * oldReplicationMetadata can be null in two cases:
     * 1. There is no value corresponding to the key
     * 2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
     * specifies that no per-record replication metadata should be persisted for batch push data.
     *
     * In such cases, the incoming Delete operation will be applied directly and we should store a tombstone for it.
     */
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (!rmdWithValueSchemaID.isPresent()) {
      final int valueSchemaID = schemaRepository.getLatestValueSchema(storeName).getId();
      GenericRecord newReplicationMetadata = newRmdCreator.apply(valueSchemaID);
      newReplicationMetadata.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
      newReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
          MergeUtils.mergeOffsetVectors(Optional.empty(), newValueSourceOffset, newValueSourceBrokerID));
      return new MergeConflictResult(Optional.empty(), valueSchemaID, false, newReplicationMetadata);
    } else if (rmdWithValueSchemaID.get().getValueSchemaId() <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = "
          + storeName + "; schema ID = " + rmdWithValueSchemaID.get().getValueSchemaId());
    }

    final GenericRecord oldReplicationMetadataRecord = rmdWithValueSchemaID.get().getReplicationMetadataRecord();
    final int valueSchemaID = rmdWithValueSchemaID.get().getValueSchemaId();
    Object tsObject = oldReplicationMetadataRecord.get(TIMESTAMP_FIELD_NAME);
    Merge.RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    if (rmdTimestampType == Merge.RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      long oldTimestamp = (long) tsObject;
      // delete wins on tie
      if (oldTimestamp <= deleteOperationTimestamp) {
        // update RMD ts
        oldReplicationMetadataRecord.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
        oldReplicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
            MergeUtils.mergeOffsetVectors(
                Optional.ofNullable((List<Long>)oldReplicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD)),
                newValueSourceOffset,
                newValueSourceBrokerID
            )
        );

        return new MergeConflictResult(Optional.empty(), valueSchemaID,false, oldReplicationMetadataRecord);
      } else { // keep the old value
        return MergeConflictResult.getIgnoredResult();
      }
    } else {
      throw new VeniceUnsupportedOperationException("Field level MD not supported");
    }

    // TODO: Handle conflict resolution for write-compute
  }
}
