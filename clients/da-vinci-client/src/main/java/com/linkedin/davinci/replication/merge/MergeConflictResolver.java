package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_POS;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;
import static com.linkedin.venice.schema.rmd.RmdTimestampType.PER_FIELD_TIMESTAMP;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_POS;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_POS;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_POS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.NO_OP_ON_FIELD;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.getFieldOperationType;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.davinci.serializer.avro.fast.MapOrderPreservingFastSerDeFactory;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdTimestampType;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.schema.writecompute.WriteComputeOperation;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;


/**
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
  private final StringAnnotatedStoreSchemaCache storeSchemaCache;
  private final Function<Integer, GenericRecord> newRmdCreator;
  private final MergeGenericRecord mergeGenericRecord;
  private final MergeByteBuffer mergeByteBuffer;
  private final MergeResultValueSchemaResolver mergeResultValueSchemaResolver;
  private final RmdSerDe rmdSerde;
  private final boolean useFieldLevelTimestamp;
  private final boolean fastAvroEnabled;

  private final SparseConcurrentList<RecordSerializer<GenericRecord>> serializerIndexedByValueSchemaId;
  private final BiIntKeyCache<RecordDeserializer<GenericRecord>> deserializerCacheForFullValue;
  private final BiIntKeyCache<SparseConcurrentList<RecordDeserializer<GenericRecord>>> deserializerCacheForUpdateValue;

  MergeConflictResolver(
      StringAnnotatedStoreSchemaCache storeSchemaCache,
      String storeName,
      Function<Integer, GenericRecord> newRmdCreator,
      MergeGenericRecord mergeGenericRecord,
      MergeByteBuffer mergeByteBuffer,
      MergeResultValueSchemaResolver mergeResultValueSchemaResolver,
      RmdSerDe rmdSerde,
      boolean useFieldLevelTimestamp,
      boolean fastAvroEnabled) {
    this.storeSchemaCache = Validate.notNull(storeSchemaCache);
    this.storeName = Validate.notNull(storeName);
    this.newRmdCreator = Validate.notNull(newRmdCreator);
    this.mergeGenericRecord = Validate.notNull(mergeGenericRecord);
    this.mergeResultValueSchemaResolver = Validate.notNull(mergeResultValueSchemaResolver);
    this.mergeByteBuffer = Validate.notNull(mergeByteBuffer);
    this.rmdSerde = Validate.notNull(rmdSerde);
    this.useFieldLevelTimestamp = useFieldLevelTimestamp;
    this.fastAvroEnabled = fastAvroEnabled;

    this.serializerIndexedByValueSchemaId = new SparseConcurrentList<>();
    this.deserializerCacheForFullValue = new BiIntKeyCache<>((writerSchemaId, readerSchemaId) -> {
      Schema writerSchema = getValueSchema(writerSchemaId);
      Schema readerSchema = getValueSchema(readerSchemaId);
      return this.fastAvroEnabled
          ? MapOrderPreservingFastSerDeFactory.getDeserializer(writerSchema, readerSchema)
          : MapOrderPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
    });
    this.deserializerCacheForUpdateValue =
        new BiIntKeyCache<>((writerSchemaId, readerSchemaId) -> new SparseConcurrentList<>());
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldValueBytesProvider A Lazy supplier of currently persisted value bytes.
   * @param rmdWithValueSchemaID The replication metadata of the currently persisted value and the value schema ID (or null)
   * @param newValueBytes The value in the incoming record.
   * @param putOperationTimestamp The logical timestamp of the incoming record.
   * @param newValueSchemaID The schema id of the value in the incoming record.
   * @param newValueSourcePosition The position from which the new value originates in the realtime stream.  Used to build
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
      RmdWithValueSchemaId rmdWithValueSchemaID,
      ByteBuffer newValueBytes,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final PubSubPosition newValueSourcePosition,
      final int newValueSourceBrokerID,
      final int newValueColoID) {
    if (rmdWithValueSchemaID == null) {
      // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
      return putWithoutRmd(
          newValueBytes,
          putOperationTimestamp,
          newValueSchemaID,
          newValueSourcePosition,
          newValueSourceBrokerID);
    }
    if (rmdWithValueSchemaID.getValueSchemaId() <= 0) {
      throw new VeniceException(
          "Invalid schema Id of old value found when replication metadata exists for store = " + storeName
              + "; schema ID = " + rmdWithValueSchemaID.getValueSchemaId());
    }
    final GenericRecord oldRmdRecord = rmdWithValueSchemaID.getRmdRecord();
    final Object oldTimestampObject = oldRmdRecord.get(TIMESTAMP_FIELD_POS);

    /**
     * Ideally the "useFieldLevelTimestamp" flag should be sufficient to decide here. However, since current write compute
     * flag is a store-level config, when an A/A store enabled write compute feature, it will accept incoming UPDATE message
     * without changing version level write computation flag. This is a safeguard to make sure the version ingestion won't
     * fail even though the version should be recreated / repushed with correct config setup.
     */
    if (useFieldLevelTimestamp || RmdUtils.getRmdTimestampType(oldTimestampObject).equals(PER_FIELD_TIMESTAMP)) {
      return mergePutWithFieldLevelTimestamp(
          rmdWithValueSchemaID.getValueSchemaId(),
          oldTimestampObject,
          oldValueBytesProvider,
          oldRmdRecord,
          putOperationTimestamp,
          newValueBytes,
          newValueColoID,
          newValueSourcePosition,
          newValueSourceBrokerID,
          newValueSchemaID);
    }
    return mergePutWithValueLevelTimestamp(
        oldValueBytesProvider,
        oldRmdRecord,
        putOperationTimestamp,
        newValueBytes,
        newValueColoID,
        newValueSourcePosition,
        newValueSourceBrokerID,
        newValueSchemaID);
  }

  /**
   * Perform conflict resolution when the incoming operation is a DELETE operation.
   *
   * @param rmdWithValueSchemaID The replication metadata of the currently persisted value and the value schema ID.
   * @param deleteOperationTimestamp The logical timestamp of the incoming record.
   * @param deleteOperationSourcePosition The offset from which the delete operation originates in the realtime stream.
   *                                    Used to build the ReplicationMetadata for the newly inserted record.
   * @param deleteOperationSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @param deleteOperationColoID ID of the colo/fabric where this new Delete request came from.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult delete(
      Lazy<ByteBuffer> oldValueBytesProvider,
      RmdWithValueSchemaId rmdWithValueSchemaID,
      final long deleteOperationTimestamp,
      final PubSubPosition deleteOperationSourcePosition,
      final int deleteOperationSourceBrokerID,
      final int deleteOperationColoID) {
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (rmdWithValueSchemaID == null) {
      return deleteWithoutRmd(deleteOperationTimestamp, deleteOperationSourcePosition, deleteOperationSourceBrokerID);
    }
    final int oldValueSchemaID = rmdWithValueSchemaID.getValueSchemaId();
    if (oldValueSchemaID <= 0) {
      throw new VeniceException(
          "Invalid schema ID of old value found when replication metadata exists for store " + storeName
              + "; invalid value schema ID: " + oldValueSchemaID);
    }

    final GenericRecord oldRmdRecord = rmdWithValueSchemaID.getRmdRecord();
    final Object oldTimestampObject = oldRmdRecord.get(TIMESTAMP_FIELD_POS);
    /**
     * Ideally the "useFieldLevelTimestamp" flag should be sufficient to decide here. However, since current write compute
     * flag is a store-level config, when an A/A store enabled write compute feature, it will accept incoming UPDATE message
     * without changing version level write computation flag. This is a safeguard to make sure the version ingestion won't
     * fail even though the version should be recreated / repushed with correct config setup.
     */
    if (useFieldLevelTimestamp || RmdUtils.getRmdTimestampType(oldTimestampObject).equals(PER_FIELD_TIMESTAMP)) {
      return mergeDeleteWithFieldLevelTimestamp(
          oldValueBytesProvider,
          oldTimestampObject,
          oldValueSchemaID,
          oldRmdRecord,
          deleteOperationColoID,
          deleteOperationTimestamp,
          deleteOperationSourcePosition,
          deleteOperationSourceBrokerID);
    }
    return mergeDeleteWithValueLevelTimestamp(
        oldValueSchemaID,
        oldRmdRecord,
        deleteOperationColoID,
        deleteOperationTimestamp,
        deleteOperationSourcePosition,
        deleteOperationSourceBrokerID);
  }

  public MergeConflictResult update(
      Lazy<ByteBuffer> oldValueBytes,
      RmdWithValueSchemaId rmdWithValueSchemaId,
      ByteBuffer updateBytes,
      final int incomingValueSchemaId,
      final int incomingUpdateProtocolVersion,
      final long updateOperationTimestamp,
      final PubSubPosition newValueSourcePosition,
      final int newValueSourceBrokerID,
      final int newValueColoID,
      ChunkedValueManifestContainer oldValueManifest) {
    final SchemaEntry supersetValueSchemaEntry = storeSchemaCache.getSupersetSchema();
    if (supersetValueSchemaEntry == null) {
      throw new IllegalStateException("Expect to get superset value schema for store: " + storeName);
    }

    GenericRecord writeComputeRecord = deserializeWriteComputeBytes(
        incomingValueSchemaId,
        supersetValueSchemaEntry.getId(),
        incomingUpdateProtocolVersion,
        updateBytes);
    if (ignoreNewUpdate(updateOperationTimestamp, writeComputeRecord, rmdWithValueSchemaId)) {
      return MergeConflictResult.getIgnoredResult();
    }
    ValueAndRmd<GenericRecord> oldValueAndRmd = prepareValueAndRmdForUpdate(
        oldValueBytes.get(),
        rmdWithValueSchemaId,
        supersetValueSchemaEntry,
        oldValueManifest);

    int oldValueSchemaID = oldValueAndRmd.getValueSchemaId();
    if (oldValueSchemaID == -1) {
      oldValueSchemaID = supersetValueSchemaEntry.getId();
    }
    Schema oldValueSchema = getValueSchema(oldValueSchemaID);
    ValueAndRmd<GenericRecord> updatedValueAndRmd = mergeGenericRecord.update(
        oldValueAndRmd,
        Lazy.of(() -> writeComputeRecord),
        oldValueSchema,
        updateOperationTimestamp,
        newValueColoID,
        newValueSourcePosition,
        newValueSourceBrokerID);
    if (updatedValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    }
    final ByteBuffer updatedValueBytes = updatedValueAndRmd.getValue() == null
        ? null
        : serializeMergedValueRecord(oldValueSchemaID, updatedValueAndRmd.getValue());
    return new MergeConflictResult(
        updatedValueBytes,
        Optional.of(updatedValueAndRmd.getValue()),
        oldValueSchemaID,
        false,
        updatedValueAndRmd.getRmd());
  }

  private MergeConflictResult mergePutWithValueLevelTimestamp(
      Lazy<ByteBuffer> oldValueBytesProvider,
      GenericRecord oldRmdRecord,
      long putOperationTimestamp,
      ByteBuffer newValueBytes,
      int newValueColoID,
      PubSubPosition newValueSourcePosition,
      int newValueSourceBrokerID,
      int newValueSchemaID) {
    ValueAndRmd<ByteBuffer> mergedByteValueAndRmd = mergeByteBuffer.put(
        new ValueAndRmd<>(oldValueBytesProvider, oldRmdRecord),
        newValueBytes,
        putOperationTimestamp,
        newValueColoID,
        newValueSourcePosition,
        newValueSourceBrokerID);
    if (mergedByteValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    } else {
      return new MergeConflictResult(
          mergedByteValueAndRmd.getValue(),
          newValueSchemaID,
          true,
          mergedByteValueAndRmd.getRmd());
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
      PubSubPosition newValueSourcePubSubPosition,
      int newValueSourceBrokerID,
      int newValueSchemaID) {

    if (oldTimestampObject instanceof GenericRecord) {
      final GenericRecord oldValueFieldTimestampsRecord = (GenericRecord) oldTimestampObject;
      if (ignoreNewPut(oldValueSchemaID, oldValueFieldTimestampsRecord, newValueSchemaID, putOperationTimestamp)) {
        return MergeConflictResult.getIgnoredResult();
      }
    }

    final SchemaEntry mergeResultValueSchemaEntry =
        mergeResultValueSchemaResolver.getMergeResultValueSchema(oldValueSchemaID, newValueSchemaID);
    /**
     * New value record should use {@link mergeResultValueSchemaEntry} as reader schema for potential schema up-convert.
     * {@link mergeResultValueSchemaEntry} is either the same as new value schema, if old value schema is the same as
     * the new value schema, or it will be the superset schema. In the latter case, new value should be up-converted, so
     * that it contains all the fields and updated value can be properly serialized.
     */
    GenericRecord newValueRecord =
        deserializerCacheForFullValue.get(newValueSchemaID, mergeResultValueSchemaEntry.getId())
            .deserialize(newValueBytes);
    ValueAndRmd<GenericRecord> oldValueAndRmd = createOldValueAndRmd(
        mergeResultValueSchemaEntry.getSchema(),
        mergeResultValueSchemaEntry.getId(),
        oldValueSchemaID,
        oldValueBytesProvider,
        oldRmdRecord);
    // Actual merge happens here!
    ValueAndRmd<GenericRecord> mergedValueAndRmd = mergeGenericRecord.put(
        oldValueAndRmd,
        newValueRecord,
        putOperationTimestamp,
        newValueColoID,
        newValueSourcePubSubPosition,
        newValueSourceBrokerID);
    if (mergedValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    }
    ByteBuffer mergedValueBytes =
        serializeMergedValueRecord(mergeResultValueSchemaEntry.getId(), mergedValueAndRmd.getValue());
    return new MergeConflictResult(mergedValueBytes, newValueSchemaID, false, mergedValueAndRmd.getRmd());
  }

  private MergeConflictResult mergeDeleteWithValueLevelTimestamp(
      int valueSchemaID,
      GenericRecord oldRmdRecord,
      int deleteOperationColoID,
      long deleteOperationTimestamp,
      PubSubPosition newValueSourcePosition,
      int deleteOperationSourceBrokerID) {
    ValueAndRmd<ByteBuffer> valueAndRmd = new ValueAndRmd<>(
        Lazy.of(() -> null), // In this case, we do not need the current value to handle the Delete request.
        oldRmdRecord);
    ValueAndRmd<ByteBuffer> mergedValueAndRmd = mergeByteBuffer.delete(
        valueAndRmd,
        deleteOperationTimestamp,
        deleteOperationColoID,
        newValueSourcePosition,
        deleteOperationSourceBrokerID);

    if (mergedValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    } else {
      return new MergeConflictResult(null, valueSchemaID, false, oldRmdRecord);
    }
  }

  private MergeConflictResult mergeDeleteWithFieldLevelTimestamp(
      Lazy<ByteBuffer> oldValueBytesProvider,
      Object oldTimestampObject,
      int oldValueSchemaID,
      GenericRecord oldRmdRecord,
      int deleteOperationColoID,
      long deleteOperationTimestamp,
      PubSubPosition deleteOperationSourcePosition,
      int deleteOperationSourceBrokerID) {

    if (oldTimestampObject instanceof GenericRecord) {
      final GenericRecord oldValueFieldTimestampsRecord = (GenericRecord) oldTimestampObject;
      if (ignoreNewDelete(oldValueFieldTimestampsRecord, deleteOperationTimestamp)) {
        return MergeConflictResult.getIgnoredResult();
      }
    }
    // In this case, the writer and reader schemas are the same because deletion does not introduce any new schema.
    final Schema oldValueSchema = getValueSchema(oldValueSchemaID);
    ValueAndRmd<GenericRecord> oldValueAndRmd =
        createOldValueAndRmd(oldValueSchema, oldValueSchemaID, oldValueSchemaID, oldValueBytesProvider, oldRmdRecord);
    ValueAndRmd<GenericRecord> mergedValueAndRmd = mergeGenericRecord.delete(
        oldValueAndRmd,
        deleteOperationTimestamp,
        deleteOperationColoID,
        deleteOperationSourcePosition,
        deleteOperationSourceBrokerID);
    if (mergedValueAndRmd.isUpdateIgnored()) {
      return MergeConflictResult.getIgnoredResult();
    }
    final ByteBuffer mergedValueBytes = mergedValueAndRmd.getValue() == null
        ? null
        : serializeMergedValueRecord(oldValueSchemaID, mergedValueAndRmd.getValue());
    return new MergeConflictResult(mergedValueBytes, oldValueSchemaID, false, mergedValueAndRmd.getRmd());
  }

  /**
   * This method create a pair of deserialized value of type {@link GenericRecord} and its corresponding replication metadata.
   * It takes into account the writer schema and reader schema. If the writer schema is different from the reader schema,
   * the replication metadata record will be converted to use the RMD schema generated from the reader schema.
   *
   * @param readerValueSchema reader schema.
   * @param readerValueSchemaID reader schema ID.
   * @param oldValueWriterSchemaID writer schema ID of the old value.
   * @param oldValueBytesProvider provides old value bytes.
   * @param oldRmdRecord Replication metadata record that has the RMD schema generated from the writer value schema.
   * @return a pair of deserialized value of type {@link GenericRecord} and its corresponding replication metadata.
   */
  private ValueAndRmd<GenericRecord> createOldValueAndRmd(
      Schema readerValueSchema,
      int readerValueSchemaID,
      int oldValueWriterSchemaID,
      Lazy<ByteBuffer> oldValueBytesProvider,
      GenericRecord oldRmdRecord) {
    final GenericRecord oldValueRecord = createValueRecordFromByteBuffer(
        readerValueSchema,
        readerValueSchemaID,
        oldValueWriterSchemaID,
        oldValueBytesProvider.get());

    // RMD record should contain a per-field timestamp and it should use the RMD schema generated from
    // mergeResultValueSchema.
    oldRmdRecord = convertToPerFieldTimestampRmd(oldRmdRecord, oldValueRecord);
    if (readerValueSchemaID != oldValueWriterSchemaID) {
      oldRmdRecord = convertRmdToUseReaderValueSchema(readerValueSchemaID, oldValueWriterSchemaID, oldRmdRecord);
    }
    ValueAndRmd<GenericRecord> createdOldValueAndRmd = new ValueAndRmd<>(Lazy.of(() -> oldValueRecord), oldRmdRecord);
    createdOldValueAndRmd.setValueSchemaId(readerValueSchemaID);
    return createdOldValueAndRmd;
  }

  private GenericRecord createValueRecordFromByteBuffer(
      Schema readerValueSchema,
      int readerValueSchemaID,
      int oldValueWriterSchemaID,
      ByteBuffer oldValueBytes) {
    if (oldValueBytes == null) {
      return AvroSchemaUtils.createGenericRecord(readerValueSchema);
    }
    return deserializerCacheForFullValue.get(oldValueWriterSchemaID, readerValueSchemaID).deserialize(oldValueBytes);
  }

  private GenericRecord convertRmdToUseReaderValueSchema(
      final int readerValueSchemaID,
      final int writerValueSchemaID,
      GenericRecord oldRmdRecord) {
    if (readerValueSchemaID == writerValueSchemaID) {
      // No need to convert the record to use a different schema.
      return oldRmdRecord;
    }
    final ByteBuffer rmdBytes = rmdSerde.serializeRmdRecord(writerValueSchemaID, oldRmdRecord);
    return rmdSerde.deserializeRmdBytes(writerValueSchemaID, readerValueSchemaID, rmdBytes);
  }

  private boolean ignoreNewPut(
      final int oldValueSchemaID,
      GenericRecord oldValueFieldTimestampsRecord,
      final int newValueSchemaID,
      final long putOperationTimestamp) {
    final Schema oldValueSchema = getValueSchema(oldValueSchemaID);
    List<Schema.Field> oldValueFields = oldValueSchema.getFields();

    if (oldValueSchemaID == newValueSchemaID) {
      for (Schema.Field field: oldValueFields) {
        if (isRmdFieldTimestampSmaller(oldValueFieldTimestampsRecord, field.name(), putOperationTimestamp, false)) {
          return false;
        }
      }
      // All timestamps of existing fields are strictly greater than the new put timestamp. So, new Put can be ignored.
      return true;

    } else {
      Schema newValueSchema = getValueSchema(newValueSchemaID);
      Set<String> oldFieldNames = oldValueFields.stream().map(Schema.Field::name).collect(Collectors.toSet());
      Set<String> newFieldNames =
          newValueSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());

      if (oldFieldNames.containsAll(newFieldNames)) {
        // New value fields set is a subset of existing/old value fields set.
        for (String newFieldName: newFieldNames) {
          if (isRmdFieldTimestampSmaller(oldValueFieldTimestampsRecord, newFieldName, putOperationTimestamp, false)) {
            return false;
          }
        }
        // All timestamps of existing fields are strictly greater than the new put timestamp. So, new Put can be
        // ignored.
        return true;

      } else {
        // Should not ignore new value because it contains field(s) that the existing value does not contain.
        return false;
      }
    }
  }

  private boolean ignoreNewDelete(GenericRecord oldValueFieldTimestampsRecord, final long deleteOperationTimestamp) {
    for (Schema.Field field: oldValueFieldTimestampsRecord.getSchema().getFields()) {
      if (isRmdFieldTimestampSmaller(oldValueFieldTimestampsRecord, field.name(), deleteOperationTimestamp, false)) {
        return false;
      }
    }
    return true;
  }

  private Schema getValueSchema(final int valueSchemaID) {
    return storeSchemaCache.getValueSchema(valueSchemaID).getSchema();
  }

  private Schema getWriteComputeSchema(final int valueSchemaID, final int writeComputeSchemaID) {
    return storeSchemaCache.getDerivedSchema(valueSchemaID, writeComputeSchemaID).getSchema();
  }

  private boolean isRmdFieldTimestampSmaller(
      GenericRecord oldValueFieldTimestampsRecord,
      String fieldName,
      final long newTimestamp,
      final boolean strictlySmaller) {
    final Object fieldTimestampObj = oldValueFieldTimestampsRecord.get(fieldName);
    final long oldFieldTimestamp;
    if (fieldTimestampObj instanceof Long) {
      oldFieldTimestamp = (Long) fieldTimestampObj;
    } else if (fieldTimestampObj instanceof GenericRecord) {
      oldFieldTimestamp = (Long) ((GenericRecord) fieldTimestampObj).get(TOP_LEVEL_TS_FIELD_POS);
    } else {
      throw new VeniceException(
          "Replication metadata field timestamp is expected to be either a long or a GenericRecord. " + "Got: "
              + fieldTimestampObj);
    }
    return strictlySmaller ? (oldFieldTimestamp < newTimestamp) : (oldFieldTimestamp <= newTimestamp);
  }

  private MergeConflictResult putWithoutRmd(
      ByteBuffer newValue,
      final long putOperationTimestamp,
      final int newValueSchemaID,
      final PubSubPosition newValueSourcePosition,
      final int newValueSourceBrokerID) {
    /**
     * Replication metadata could be null in two cases:
     *    1. There is no value corresponding to the key
     *    2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
     *
     * Specifies that no per-record replication metadata should be persisted for batch push data.
     * In such cases, the incoming PUT operation will be applied directly and we should store the updated RMD for it.
     */
    GenericRecord newRmd = newRmdCreator.apply(newValueSchemaID);
    newRmd.put(TIMESTAMP_FIELD_POS, putOperationTimestamp);
    // A record which didn't come from an RT topic or has null metadata should have no offset vector.
    newRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS, Collections.emptyList());

    if (useFieldLevelTimestamp) {
      Schema valueSchema = getValueSchema(newValueSchemaID);
      newRmd = createOldValueAndRmd(valueSchema, newValueSchemaID, newValueSchemaID, Lazy.of(() -> newValue), newRmd)
          .getRmd();
    }
    return new MergeConflictResult(newValue, newValueSchemaID, true, newRmd);
  }

  private MergeConflictResult deleteWithoutRmd(
      long deleteOperationTimestamp,
      PubSubPosition newValueSourcePosition,
      int deleteOperationSourceBrokerID) {
    /**
     * oldReplicationMetadata can be null in two cases:
     * 1. There is no value corresponding to the key
     * 2. There is a value corresponding to the key, but it came from the batch push and the BatchConflictResolutionPolicy
     * specifies that no per-record replication metadata should be persisted for batch push data.
     *
     * In such cases, the incoming Delete operation will be applied directly and we should store a tombstone for it.
     */
    final int valueSchemaID = storeSchemaCache.getSupersetOrLatestValueSchema().getId();
    GenericRecord newRmd = newRmdCreator.apply(valueSchemaID);
    newRmd.put(TIMESTAMP_FIELD_POS, deleteOperationTimestamp);
    newRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS, Collections.emptyList());
    if (useFieldLevelTimestamp) {
      Schema valueSchema = getValueSchema(valueSchemaID);
      newRmd = createOldValueAndRmd(valueSchema, valueSchemaID, valueSchemaID, Lazy.of(() -> null), newRmd).getRmd();
    }
    return new MergeConflictResult(null, valueSchemaID, false, newRmd);
  }

  private GenericRecord deserializeWriteComputeBytes(
      int writerValueSchemaId,
      int readerValueSchemaId,
      int updateProtocolVersion,
      ByteBuffer updateBytes) {
    RecordDeserializer<GenericRecord> deserializer =
        deserializerCacheForUpdateValue.get(writerValueSchemaId, readerValueSchemaId)
            .computeIfAbsent(updateProtocolVersion, ignored -> {
              Schema writerSchema = getWriteComputeSchema(writerValueSchemaId, updateProtocolVersion);
              Schema readerSchema = getWriteComputeSchema(readerValueSchemaId, updateProtocolVersion);
              return this.fastAvroEnabled
                  ? MapOrderPreservingFastSerDeFactory.getDeserializer(writerSchema, readerSchema)
                  : MapOrderPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
            });

    return deserializer.deserialize(updateBytes);
  }

  private ValueAndRmd<GenericRecord> prepareValueAndRmdForUpdate(
      ByteBuffer oldValueBytes,
      RmdWithValueSchemaId rmdWithValueSchemaId,
      SchemaEntry readerValueSchemaSchemaEntry,
      ChunkedValueManifestContainer oldValueManifest) {

    if (rmdWithValueSchemaId == null) {
      GenericRecord newValue;
      if (oldValueBytes == null) {
        // Value and RMD both never existed
        newValue = AvroSchemaUtils.createGenericRecord(readerValueSchemaSchemaEntry.getSchema());
      } else {
        /**
         * RMD does not exist. This means the value is written in Batch phase and does not have RMD associated. Records
         * should have the schema id in the first few bytes unless they are assembled from a chunked value. In order
         * to provide coverage for both cases, we just utilize the supersetSchema entry which should be the latest
         * schema (and therefore should not drop fields after the update).
         */

        int schemaId;
        if (oldValueManifest != null && oldValueManifest.getManifest() != null) {
          schemaId = oldValueManifest.getManifest().getSchemaId();
        } else {
          schemaId = ValueRecord.parseSchemaId(oldValueBytes.array());
        }
        newValue = deserializerCacheForFullValue.get(schemaId, readerValueSchemaSchemaEntry.getId())
            .deserialize(oldValueBytes);
      }
      GenericRecord newRmd = newRmdCreator.apply(readerValueSchemaSchemaEntry.getId());
      newRmd.put(TIMESTAMP_FIELD_POS, createPerFieldTimestampRecord(newRmd.getSchema(), 0L, newValue));
      newRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS, Collections.emptyList());
      return new ValueAndRmd<>(Lazy.of(() -> newValue), newRmd);
    }

    int oldValueWriterSchemaId = rmdWithValueSchemaId.getValueSchemaId();
    return createOldValueAndRmd(
        readerValueSchemaSchemaEntry.getSchema(),
        readerValueSchemaSchemaEntry.getId(),
        oldValueWriterSchemaId,
        Lazy.of(() -> oldValueBytes),
        rmdWithValueSchemaId.getRmdRecord());
  }

  private GenericRecord convertToPerFieldTimestampRmd(GenericRecord rmd, GenericRecord oldValueRecord) {
    Object timestampObject = rmd.get(TIMESTAMP_FIELD_POS);
    RmdTimestampType timestampType = RmdUtils.getRmdTimestampType(timestampObject);
    switch (timestampType) {
      case PER_FIELD_TIMESTAMP:
        // Nothing needs to happen in this case.
        return rmd;

      case VALUE_LEVEL_TIMESTAMP:
        GenericRecord perFieldTimestampRecord =
            createPerFieldTimestampRecord(rmd.getSchema(), (long) timestampObject, oldValueRecord);
        rmd.put(TIMESTAMP_FIELD_POS, perFieldTimestampRecord);
        return rmd;

      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + timestampType);
    }
  }

  protected GenericRecord createPerFieldTimestampRecord(
      Schema rmdSchema,
      long fieldTimestamp,
      GenericRecord oldValueRecord) {
    Schema perFieldTimestampRecordSchema = rmdSchema.getFields().get(TIMESTAMP_FIELD_POS).schema().getTypes().get(1);
    // Per-field timestamp record schema should have default timestamp values.
    GenericRecord perFieldTimestampRecord = AvroSchemaUtils.createGenericRecord(perFieldTimestampRecordSchema);
    for (Schema.Field field: perFieldTimestampRecordSchema.getFields()) {
      Schema.Type timestampFieldType = field.schema().getType();
      switch (timestampFieldType) {
        case LONG:
          perFieldTimestampRecord.put(field.pos(), fieldTimestamp);
          continue;

        case RECORD:
          GenericRecord collectionFieldTimestampRecord = AvroSchemaUtils.createGenericRecord(field.schema());
          // Only need to set the top-level field timestamp on collection timestamp record.
          collectionFieldTimestampRecord.put(TOP_LEVEL_TS_FIELD_POS, fieldTimestamp);
          // When a collection field metadata is created, its top-level colo ID is always -1.
          collectionFieldTimestampRecord.put(TOP_LEVEL_COLO_ID_FIELD_POS, -1);
          collectionFieldTimestampRecord
              .put(PUT_ONLY_PART_LENGTH_FIELD_POS, getCollectionFieldLen(oldValueRecord, field.name()));
          perFieldTimestampRecord.put(field.pos(), collectionFieldTimestampRecord);
          continue;

        default:
          throw new VeniceException(
              "Unsupported timestamp field type: " + timestampFieldType + ", timestamp record schema: "
                  + perFieldTimestampRecordSchema);
      }
    }
    return perFieldTimestampRecord;
  }

  private int getCollectionFieldLen(GenericRecord valueRecord, String collectionFieldName) {
    Object collectionFieldValue = valueRecord.get(collectionFieldName);
    if (collectionFieldValue == null) {
      return 0;
    }
    if (collectionFieldValue instanceof List) {
      return ((List<?>) collectionFieldValue).size();

    } else if (collectionFieldValue instanceof Map) {
      return ((Map<?, ?>) collectionFieldValue).size();

    } else {
      throw new IllegalStateException(
          "Expect field " + collectionFieldName + " to be a collection field. But got: "
              + collectionFieldValue.getClass());
    }
  }

  private boolean ignoreNewUpdate(
      final long updateOperationTimestamp,
      GenericRecord writeComputeRecord,
      RmdWithValueSchemaId rmdWithValueSchemaId) {
    if (rmdWithValueSchemaId == null) {
      return false;
    }
    if (!WriteComputeOperation.isPartialUpdateOp(writeComputeRecord)) {
      // This Write Compute record could be a Write Compute Delete request which is not supported and there should be no
      // one using it.
      throw new IllegalStateException(
          "Write Compute only support partial update. Got unexpected Write Compute record: " + writeComputeRecord);
    }

    Object oldTimestampObject = rmdWithValueSchemaId.getRmdRecord().get(TIMESTAMP_FIELD_POS);
    Schema oldValueSchema = getValueSchema(rmdWithValueSchemaId.getValueSchemaId());
    RmdTimestampType rmdTimestampType = RmdUtils.getRmdTimestampType(oldTimestampObject);
    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        final long valueLevelTimestamp = (long) oldTimestampObject;
        if (updateOperationTimestamp >= valueLevelTimestamp) {
          return false;
        }
        for (Schema.Field field: writeComputeRecord.getSchema().getFields()) {
          if (getFieldOperationType(writeComputeRecord.get(field.pos())) != NO_OP_ON_FIELD
              && oldValueSchema.getField(field.name()) == null) {
            return false; // Write Compute tries to update a non-existing field in the old value (schema).
          }
        }
        return true; // Write Compute does not try to update any non-existing fields in the old value (schema).

      case PER_FIELD_TIMESTAMP:
        GenericRecord oldTimestampRecord = (GenericRecord) oldTimestampObject;
        for (Schema.Field field: writeComputeRecord.getSchema().getFields()) {
          if (!oldTimestampRecord.hasField(field.name())) {
            if (getFieldOperationType(writeComputeRecord.get(field.pos())) == NO_OP_ON_FIELD) {
              continue; // New field does not perform actual update.
            }
            return false; // Partial update tries to update a non-existing field.
          }
          if (isRmdFieldTimestampSmaller(oldTimestampRecord, field.name(), updateOperationTimestamp, false)) {
            return false; // One existing field must be updated.
          }
        }
        return true;

      default:
        throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + rmdTimestampType);
    }
  }

  private ByteBuffer serializeMergedValueRecord(int mergedValueSchemaId, GenericRecord mergedValue) {
    // TODO: avoid serializing the merged value result here and instead serializing it before persisting it. The goal
    // is to avoid back-and-forth ser/de. Because when the merged result is read before it is persisted, we may need
    // to deserialize it.
    RecordSerializer serializer = serializerIndexedByValueSchemaId.computeIfAbsent(mergedValueSchemaId, ignored -> {
      Schema mergedValueSchema = getValueSchema(mergedValueSchemaId);
      return fastAvroEnabled
          ? MapOrderPreservingFastSerDeFactory.getSerializer(mergedValueSchema)
          : MapOrderPreservingSerDeFactory.getSerializer(mergedValueSchema);
    });
    return ByteBuffer.wrap(serializer.serialize(mergedValue));
  }
}
