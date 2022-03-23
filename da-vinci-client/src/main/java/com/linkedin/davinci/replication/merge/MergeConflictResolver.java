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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;
import static com.linkedin.venice.schema.rmd.v1.CollectionReplicationMetadata.*;


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
   * @param rmdWithValueSchemaIdOptional The replication metadata of the currently persisted value and the value schema ID.
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
      Optional<ReplicationMetadataWithValueSchemaId> rmdWithValueSchemaIdOptional,
      ByteBuffer newValue,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      final int newValueColoID
  ) {
    if (!rmdWithValueSchemaIdOptional.isPresent()) {
      // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
      return putWithNoReplicationMetadata(newValue, putOperationTimestamp, newValueSchemaID, newValueSourceOffset, newValueSourceBrokerID);
    }
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaID = rmdWithValueSchemaIdOptional.get();
    if (rmdWithValueSchemaID.getValueSchemaId() <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = "
          + storeName + "; schema ID = " + rmdWithValueSchemaID.getValueSchemaId());
    }

    final GenericRecord oldRmd = rmdWithValueSchemaID.getReplicationMetadataRecord();
    Object oldTimestampObject = oldRmd.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(oldTimestampObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        ValueAndReplicationMetadata<ByteBuffer> valueAndRmd = new ValueAndReplicationMetadata<>(oldValue, oldRmd);
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
        final int oldValueSchemaID = rmdWithValueSchemaID.getValueSchemaId();
        final GenericRecord oldValueFieldTimestampsRecord =
            (GenericRecord) rmdWithValueSchemaID.getReplicationMetadataRecord().get(TIMESTAMP_FIELD_NAME);
        if (ignoreNewPut(oldValueSchemaID, oldValueFieldTimestampsRecord, newValueSchemaID, putOperationTimestamp)) {
          return MergeConflictResult.getIgnoredResult();
        }
        throw new VeniceUnsupportedOperationException("Field level RMD timestamp not-ignoring-new-put situation not supported.");
        /**
         * TODO Implement following steps:
         *    1. Read old value from RocksDB.
         *    2. Deserialize old value bytes to a GenericRecord.
         *    3. Deserialize new value bytes to a GenericRecord.
         *    4. Look at both schemas and decide if the superset schema should be used for the merged value and RMD.
         *       If so, do it.
         *    5. Call mergeGenericRecord.put(...)
         */

      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + rmdTimestampType);
    }
  }

  // Visit for testing
  boolean ignoreNewPut(
      final int oldValueSchemaID,
      GenericRecord oldValueFieldTimestampsRecord,
      final int newValueSchemaID,
      final long putOperationTimestamp
  ) {
    Schema oldValueSchema = schemaRepository.getValueSchema(storeName, oldValueSchemaID).getSchema();
    List<Schema.Field> oldValueFields = oldValueSchema.getFields();

    if (oldValueSchemaID == newValueSchemaID) {
      for (Schema.Field field : oldValueFields) {
        if (isRmdFieldTimestampSmallerOrEqual(oldValueFieldTimestampsRecord, field.name(), putOperationTimestamp)) {
          return false;
        }
      }
      // All timestamps of existing fields are strictly greater than the new put timestamp. So, new Put can be ignored.
      return true;

    } else {
      Schema newValueSchema = schemaRepository.getValueSchema(storeName, newValueSchemaID).getSchema();
      Set<String> oldFieldNames = oldValueFields.stream().map(Schema.Field::name).collect(Collectors.toSet());
      Set<String> newFieldNames = newValueSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());

      if (oldFieldNames.containsAll(newFieldNames)) {
        // New value fields set is a subset of existing/old value fields set.
        for (String newFieldName : newFieldNames) {
          if (isRmdFieldTimestampSmallerOrEqual(oldValueFieldTimestampsRecord, newFieldName, putOperationTimestamp)) {
            return false;
          }
        }
        // All timestamps of existing fields are strictly greater than the new put timestamp. So, new Put can be ignored.
        return true;

      } else {
        // Should not ignore new value because it contains field(s) that the existing value does not contain.
        return false;
      }
    }
  }

  private boolean isRmdFieldTimestampSmallerOrEqual(
      GenericRecord oldValueFieldTimestampsRecord,
      String fieldName,
      final long newTimestamp
  ) {
    final Object fieldTimestampObj = oldValueFieldTimestampsRecord.get(fieldName);
    final long oldFieldTimestamp;
    if (fieldTimestampObj instanceof Long) {
      oldFieldTimestamp = (Long) fieldTimestampObj;
    } else if (fieldTimestampObj instanceof GenericRecord) {
      oldFieldTimestamp = (Long) ((GenericRecord) fieldTimestampObj).get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME);
    } else {
      throw new VeniceException("Replication metadata field timestamp is expected to be either a long or a GenericRecord. "
          + "Got: " + fieldTimestampObj);
    }
    return oldFieldTimestamp <= newTimestamp;
  }

  private MergeConflictResult putWithNoReplicationMetadata(
      ByteBuffer newValue,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID
  ) {
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
  }

  /**
   * Perform conflict resolution when the incoming operation is a DELETE operation.
   *
   * @param rmdWithValueSchemaID The replication metadata of the currently persisted value and the value schema ID.
   * @param deleteOperationTimestamp The logical timestamp of the incoming record.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param deleteOperationSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @param deleteOperationColoID ID of the colo/fabric where this new Delete request came from.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult delete(
      Optional<ReplicationMetadataWithValueSchemaId> rmdWithValueSchemaID,
      final long deleteOperationTimestamp,
      final long newValueSourceOffset,
      final int deleteOperationSourceBrokerID,
      final int deleteOperationColoID
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
          MergeUtils.mergeOffsetVectors(Optional.empty(), newValueSourceOffset, deleteOperationSourceBrokerID));
      return new MergeConflictResult(Optional.empty(), valueSchemaID, false, newReplicationMetadata);
    } else if (rmdWithValueSchemaID.get().getValueSchemaId() <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = "
          + storeName + "; schema ID = " + rmdWithValueSchemaID.get().getValueSchemaId());
    }

    final GenericRecord oldReplicationMetadataRecord = rmdWithValueSchemaID.get().getReplicationMetadataRecord();
    final int valueSchemaID = rmdWithValueSchemaID.get().getValueSchemaId();
    Object tsObject = oldReplicationMetadataRecord.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      ValueAndReplicationMetadata<ByteBuffer> valueAndRmd = new ValueAndReplicationMetadata<>(
          Lazy.of(() -> null), // Current value can be passed as null because it is not needed to handle the Delete request.
          oldReplicationMetadataRecord
      );
      valueAndRmd = mergeByteBuffer.delete(
          valueAndRmd,
          deleteOperationTimestamp,
          deleteOperationColoID,
          newValueSourceOffset,
          deleteOperationSourceBrokerID
      );

      if (valueAndRmd.isUpdateIgnored()) {
        return MergeConflictResult.getIgnoredResult();
      } else {
        return new MergeConflictResult(Optional.empty(), valueSchemaID,false, oldReplicationMetadataRecord);
      }

    } else {
      throw new VeniceUnsupportedOperationException("Field level MD not supported");
    }

    // TODO: Handle conflict resolution for write-compute
  }
}
