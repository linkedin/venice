package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.serialization.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;

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
@Threadsafe
public class MergeConflictResolver {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepository;
  private final Function<Integer, GenericRecord> newRmdCreator;
  private final MergeGenericRecord mergeGenericRecord;
  private final MergeByteBuffer mergeByteBuffer;
  private final MergeResultValueSchemaResolver mergeResultValueSchemaResolver;
  private final ReplicationMetadataSerDe rmdSerde;

  MergeConflictResolver(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      Function<Integer, GenericRecord> newRmdCreator,
      MergeGenericRecord mergeGenericRecord,
      MergeByteBuffer mergeByteBuffer,
      MergeResultValueSchemaResolver mergeResultValueSchemaResolver,
      ReplicationMetadataSerDe rmdSerde
  ) {
    this.schemaRepository = Validate.notNull(schemaRepository);
    this.storeName = Validate.notNull(storeName);
    this.newRmdCreator = Validate.notNull(newRmdCreator);
    this.mergeGenericRecord = Validate.notNull(mergeGenericRecord);
    this.mergeResultValueSchemaResolver = Validate.notNull(mergeResultValueSchemaResolver);
    this.mergeByteBuffer = Validate.notNull(mergeByteBuffer);
    this.rmdSerde = Validate.notNull(rmdSerde);
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldValueBytesProvider A Lazy supplier of currently persisted value bytes.
   * @param rmdWithValueSchemaIdOptional The replication metadata of the currently persisted value and the value schema ID.
   * @param newValueBytes The value in the incoming record.
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
      Lazy<ByteBuffer> oldValueBytesProvider,
      Optional<ReplicationMetadataWithValueSchemaId> rmdWithValueSchemaIdOptional,
      ByteBuffer newValueBytes,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      final int newValueColoID
  ) {
    if (!rmdWithValueSchemaIdOptional.isPresent()) {
      // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
      return putWithoutReplicationMetadata(newValueBytes, putOperationTimestamp, newValueSchemaID, newValueSourceOffset, newValueSourceBrokerID);
    }
    ReplicationMetadataWithValueSchemaId rmdWithValueSchemaID = rmdWithValueSchemaIdOptional.get();
    if (rmdWithValueSchemaID.getValueSchemaId() <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = "
          + storeName + "; schema ID = " + rmdWithValueSchemaID.getValueSchemaId());
    }
    final GenericRecord oldRmdRecord = rmdWithValueSchemaID.getReplicationMetadataRecord();
    final Object oldTimestampObject = oldRmdRecord.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(oldTimestampObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        return mergePutWithValueLevelTimestamp(
            oldValueBytesProvider,
            oldRmdRecord,
            putOperationTimestamp,
            newValueBytes,
            newValueColoID,
            newValueSourceOffset,
            newValueSourceBrokerID,
            newValueSchemaID
        );

      case PER_FIELD_TIMESTAMP:
        return mergePutWithFieldLevelTimestamp(
            rmdWithValueSchemaID.getValueSchemaId(),
            oldTimestampObject,
            oldValueBytesProvider,
            oldRmdRecord,
            putOperationTimestamp,
            newValueBytes,
            newValueColoID,
            newValueSourceOffset,
            newValueSourceBrokerID,
            newValueSchemaID
        );

      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + rmdTimestampType);
    }
  }

  private MergeConflictResult mergePutWithValueLevelTimestamp(
      Lazy<ByteBuffer> oldValueBytesProvider,
      GenericRecord oldRmdRecord,
      long putOperationTimestamp,
      ByteBuffer newValueBytes,
      int newValueColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID,
      int newValueSchemaID
  ) {
    ValueAndReplicationMetadata<ByteBuffer> mergedByteValueAndRmd = mergeByteBuffer.put(
        new ValueAndReplicationMetadata<>(oldValueBytesProvider, oldRmdRecord),
        newValueBytes,
        putOperationTimestamp,
        newValueColoID,
        newValueSourceOffset,
        newValueSourceBrokerID
    );
    if (mergedByteValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    } else {
      return new MergeConflictResult(Optional.ofNullable(mergedByteValueAndRmd.getValue()), newValueSchemaID, true, mergedByteValueAndRmd.getReplicationMetadata());
    }
  }

  private MergeConflictResult mergePutWithFieldLevelTimestamp(
      int oldValueSchemaID,
      Object oldTimestampObject,
      Lazy<ByteBuffer> oldValueBytesProvider,
      GenericRecord oldRmdRecord,
      long putOperationTimestamp,
      ByteBuffer newValueBytes,
      int newValueColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID,
      int newValueSchemaID
  ) {
    if (!(oldTimestampObject instanceof GenericRecord)) {
      throw new IllegalStateException("Per-field RMD timestamp must be a GenericRecord. Got: " + oldTimestampObject
          + " and store name is: " + storeName);
    }
    final GenericRecord oldValueFieldTimestampsRecord = (GenericRecord) oldTimestampObject;
    if (ignoreNewPut(oldValueSchemaID, oldValueFieldTimestampsRecord, newValueSchemaID, putOperationTimestamp)) {
      return MergeConflictResult.getIgnoredResult();
    }
    final SchemaEntry mergeResultValueSchemaEntry = mergeResultValueSchemaResolver.getMergeResultValueSchema(oldValueSchemaID, newValueSchemaID);
    final Schema mergeResultValueSchema = mergeResultValueSchemaEntry.getSchema();
    final Schema newValueWriterSchema = getValueSchema(newValueSchemaID);
    /**
     * Note that it is important that the new value record should NOT use {@link mergeResultValueSchema}.
     * {@link newValueWriterSchema} is either the same as {@link mergeResultValueSchema} or it is a subset of
     * {@link mergeResultValueSchema}.
     */
    GenericRecord newValueRecord = deserializeValue(newValueBytes, newValueWriterSchema, newValueWriterSchema);
    ValueAndReplicationMetadata<GenericRecord> oldValueAndRmd = createOldValueAndRmd(
        mergeResultValueSchemaEntry,
        oldValueSchemaID,
        oldValueBytesProvider,
        oldRmdRecord
    );
    // Actual merge happens here!
    ValueAndReplicationMetadata<GenericRecord> mergedValueAndRmd = mergeGenericRecord.put(
        oldValueAndRmd,
        newValueRecord,
        putOperationTimestamp,
        newValueColoID,
        newValueSourceOffset,
        newValueSourceBrokerID
    );

    // TODO: avoid serializing the merged value result here and instead serializing it before persisting it. The goal
    // is to avoid back-and-forth ser/de. Because when the merged result is read before it is persisted, we may need
    // to deserialize it.
    ByteBuffer mergedValueBytes = ByteBuffer.wrap(
        MapOrderingPreservingSerDeFactory.getSerializer(mergeResultValueSchema).serialize(mergedValueAndRmd.getValue())
    );
    return new MergeConflictResult(Optional.of(mergedValueBytes), newValueSchemaID, true, mergedValueAndRmd.getReplicationMetadata());
  }

  private ValueAndReplicationMetadata<GenericRecord> createOldValueAndRmd(
      SchemaEntry mergeResultValueSchemaEntry,
      int oldValueSchemaID,
      Lazy<ByteBuffer> oldValueBytesProvider,
      GenericRecord oldRmdRecord
  ) {
    final Schema mergeResultValueSchema = mergeResultValueSchemaEntry.getSchema();
    Lazy<GenericRecord> oldValueRecordProvider = Lazy.of(() -> {
      ByteBuffer oldValueBytes = oldValueBytesProvider.get();
      if (oldValueBytes == null) {
        return new GenericData.Record(mergeResultValueSchema);
      }
      final Schema oldValueWriterSchema = getValueSchema(oldValueSchemaID);
      return deserializeValue(oldValueBytes, oldValueWriterSchema, mergeResultValueSchema);
    });
    // RMD record should use the RMD schema generated from mergeResultValueSchema.
    GenericRecord expandedOldRmdRecord = mayConvertRmdToUseMergeResultSchema(mergeResultValueSchemaEntry.getId(), oldValueSchemaID, oldRmdRecord);
    return new ValueAndReplicationMetadata<>(
        oldValueRecordProvider,
        expandedOldRmdRecord
    );
  }

  private GenericRecord mayConvertRmdToUseMergeResultSchema(final int mergeResultValueSchemaID, final int oldValueSchemaID, GenericRecord oldRmdRecord) {
    if (mergeResultValueSchemaID == oldValueSchemaID) {
      return oldRmdRecord;
    }
    final ByteBuffer rmdBytes = rmdSerde.serializeRmdRecord(oldValueSchemaID, oldRmdRecord);
    return rmdSerde.deserializeRmdBytes(oldValueSchemaID, mergeResultValueSchemaID, rmdBytes);
  }

  private GenericRecord deserializeValue(ByteBuffer bytes, Schema writerSchema, Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema).deserialize(bytes);
  }

  private boolean ignoreNewPut(
      final int oldValueSchemaID,
      GenericRecord oldValueFieldTimestampsRecord,
      final int newValueSchemaID,
      final long putOperationTimestamp
  ) {
    final Schema oldValueSchema = getValueSchema(oldValueSchemaID);
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
      Schema newValueSchema = getValueSchema(newValueSchemaID);
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

  private Schema getValueSchema(final int valueSchemaID) {
    return schemaRepository.getValueSchema(storeName, valueSchemaID).getSchema();
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

  private MergeConflictResult putWithoutReplicationMetadata(
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
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (!rmdWithValueSchemaID.isPresent()) {
      return deleteWithoutReplicationMetadata(deleteOperationTimestamp, newValueSourceOffset,
          deleteOperationSourceBrokerID);
    }
    final int oldValueSchemaID = rmdWithValueSchemaID.get().getValueSchemaId();
    if (oldValueSchemaID <= 0) {
      throw new VeniceException(
          "Invalid schema ID of old value found when replication metadata exists for store " + storeName + "; invalid value schema ID: " + oldValueSchemaID);
    }

    final GenericRecord oldRmdRecord = rmdWithValueSchemaID.get().getReplicationMetadataRecord();
    final Object tsObject = oldRmdRecord.get(TIMESTAMP_FIELD_NAME);
    final RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        return mergeDeleteWithValueLevelTimestamp(
            oldValueSchemaID,
            oldRmdRecord,
            deleteOperationColoID,
            deleteOperationTimestamp,
            newValueSourceOffset,
            deleteOperationSourceBrokerID
        );
      case PER_FIELD_TIMESTAMP:
        // TODO: implement
      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + rmdTimestampType);
    }
  }

  private MergeConflictResult deleteWithoutReplicationMetadata(
      long deleteOperationTimestamp,
      long newValueSourceOffset,
      int deleteOperationSourceBrokerID
  ) {
    /**
     * oldReplicationMetadata can be null in two cases:
     * 1. There is no value corresponding to the key
     * 2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
     * specifies that no per-record replication metadata should be persisted for batch push data.
     *
     * In such cases, the incoming Delete operation will be applied directly and we should store a tombstone for it.
     */
    final int valueSchemaID = schemaRepository.getLatestValueSchema(storeName).getId();
    GenericRecord newRmd = newRmdCreator.apply(valueSchemaID);
    newRmd.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
    newRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
        MergeUtils.mergeOffsetVectors(Optional.empty(), newValueSourceOffset, deleteOperationSourceBrokerID));
    return new MergeConflictResult(Optional.empty(), valueSchemaID, false, newRmd);
  }

  private MergeConflictResult mergeDeleteWithValueLevelTimestamp(
      int valueSchemaID,
      GenericRecord oldRmdRecord,
      int deleteOperationColoID,
      long deleteOperationTimestamp,
      long newValueSourceOffset,
      int deleteOperationSourceBrokerID
  ) {
    ValueAndReplicationMetadata<ByteBuffer> valueAndRmd = new ValueAndReplicationMetadata<>(
        Lazy.of(() -> null), // In this case, we do not need the current value to handle the Delete request.
        oldRmdRecord
    );
    ValueAndReplicationMetadata<ByteBuffer> mergedValueAndRmd = mergeByteBuffer.delete(
        valueAndRmd,
        deleteOperationTimestamp,
        deleteOperationColoID,
        newValueSourceOffset,
        deleteOperationSourceBrokerID
    );

    if (mergedValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    } else {
      return new MergeConflictResult(Optional.empty(), valueSchemaID,false, oldRmdRecord);
    }
  }

  private MergeConflictResult mergeDeleteWithFieldLevelTimestamp(
      int valueSchemaID,
      GenericRecord oldRmdRecord,
      int deleteOperationColoID,
      long deleteOperationTimestamp,
      long newValueSourceOffset,
      int deleteOperationSourceBrokerID
  ) {
    // TODO
    return null;
  }

  public MergeConflictResult update() {
    throw new VeniceUnsupportedOperationException("TODO: Implement DCR for write-compute");
  }
}
