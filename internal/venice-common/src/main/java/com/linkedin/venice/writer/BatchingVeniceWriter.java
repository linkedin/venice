package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a batching implementation of {@link VeniceWriter}.
 * The intention of this class is to:
 * (1) Reduce message volumes sent to Venice backend.
 * (2) Avoid generating messages for the same key with the same timestamp for the same writer. Today's Active/Active DCR
 * algorithm will compare field value to break tie when two messages arrive with the same timestamp. This makes sure this
 * case will not happen if each key is only produced by single writer and user will always see the latest value in the
 * producing order.
 *
 * There are two configs that control the batching behavior:
 * (1) Max batch interval: Maximum delay of a batch of records before it is produced.
 * (2) Max batch buffer size: Maximum size of buffer records before it is produced.
 * If any of the limit is reached, the buffered batch will be flushed and produced.
 * For messages within the same batch, only the last one will be produced into the topic, except for message with logical
 * timestamp: It is against the design goal for this feature and hence it will be sent out individually.
 *
 * When the last message is produced, its callback will be completed (either successfully or exceptionally), all the
 * related messages' callbacks will also be completed with the same result.
 */
public class BatchingVeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  public static final Logger LOGGER = LogManager.getLogger(BatchingVeniceWriter.class);

  private final long batchIntervalInMs;
  private final int maxBatchSizeInBytes;
  private final ReentrantLock lock = new ReentrantLock();
  private final ExecutorService checkServiceExecutor = Executors.newSingleThreadExecutor();
  private final List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
  private final Map<ByteBuffer, ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
  private final VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private final VeniceKafkaSerializer keySerializer;
  private final VeniceKafkaSerializer valueSerializer;
  private final VeniceKafkaSerializer updateSerializer;
  private final WriteComputeHandlerV1 updateHandler = new WriteComputeHandlerV1();
  private final BiIntKeyCache<RecordDeserializer<GenericRecord>> deserializerCacheForFullValue;
  private final Map<Schema, Map<Schema, RecordDeserializer<GenericRecord>>> deserializerCacheForUpdateValue;
  private final Map<Schema, RecordSerializer<GenericRecord>> updateSerializerMap = new VeniceConcurrentHashMap<>();

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final SchemaFetcherBackedStoreSchemaCache storeSchemaCache;
  private volatile long lastBatchProduceMs;
  private int bufferSizeInBytes;

  public BatchingVeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter) {
    super(params.getTopicName());
    this.batchIntervalInMs = params.getBatchIntervalInMs();
    this.maxBatchSizeInBytes = params.getMaxBatchSizeInBytes();
    this.keySerializer = params.getKeyPayloadSerializer();
    this.valueSerializer = params.getValuePayloadSerializer();
    this.updateSerializer = params.getWriteComputePayloadSerializer();
    this.storeSchemaCache = new SchemaFetcherBackedStoreSchemaCache(params.getStoreSchemaFetcher());
    this.deserializerCacheForFullValue = new BiIntKeyCache<>((writerSchemaId, readerSchemaId) -> {
      Schema writerSchema = storeSchemaCache.getValueSchema(writerSchemaId);
      Schema readerSchema = storeSchemaCache.getValueSchema(readerSchemaId);
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
    });
    this.deserializerCacheForUpdateValue = new VeniceConcurrentHashMap<>();
    /**
     * We introduce an internal Venice writer with byte[] as the key type for any input key type. This is to make sure
     * internal buffer is indexed correctly when input key type is byte[]. For internal buffer index map, if key type is
     * byte[], comparison will be incorrect as byte[] is compared by reference, instead of value.
     * Since we will serialize key into byte[] in the internal buffer and later use it to produce with internal writer,
     * internal writer should only have the default No-Op key serializer.
     */
    VeniceWriterOptions internalWriterParams =
        new VeniceWriterOptions.Builder(params.getTopicName(), params).setKeyPayloadSerializer(new DefaultSerializer())
            .setValuePayloadSerializer(new DefaultSerializer())
            .setWriteComputePayloadSerializer(new DefaultSerializer())
            .build();
    this.veniceWriter = new VeniceWriter<>(internalWriterParams, props, producerAdapter);

  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(K key, long logicalTs, PubSubProducerCallback callback) {
    return addRecordToBuffer(MessageType.DELETE, key, null, null, -1, -1, callback, logicalTs);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support put method with put metadata");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support put method with put metadata");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support delete method with delete metadata");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(K key, PubSubProducerCallback callback) {
    return delete(key, APP_DEFAULT_LOGICAL_TS, callback);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTs,
      PubSubProducerCallback callback) {
    return addRecordToBuffer(MessageType.PUT, key, value, null, valueSchemaId, -1, callback, logicalTs);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return put(key, value, valueSchemaId, APP_DEFAULT_LOGICAL_TS, callback);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    return update(key, update, valueSchemaId, derivedSchemaId, APP_DEFAULT_LOGICAL_TS, callback);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    return addRecordToBuffer(
        MessageType.UPDATE,
        key,
        null,
        update,
        valueSchemaId,
        derivedSchemaId,
        callback,
        logicalTimestamp);
  }

  @Override
  public void flush() {
    checkAndMaybeProduceBatchRecord();
    getVeniceWriter().flush();
  }

  @Override
  public void close(boolean gracefulClose) {
    isRunning.set(false);
    if (gracefulClose) {
      checkServiceExecutor.shutdown();
      try {
        if (!checkServiceExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          checkServiceExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
    } else {
      checkServiceExecutor.shutdownNow();
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  private void periodicCheckTask() {
    while (isRunning.get()) {
      long sleepIntervalInMs = batchIntervalInMs;
      try {
        long timeSinceLastBatchProduce = System.currentTimeMillis() - lastBatchProduceMs;
        if (timeSinceLastBatchProduce >= batchIntervalInMs) {
          checkAndMaybeProduceBatchRecord();
        } else {
          // This can happen when previous batch produce time was triggered by buffer full.
          sleepIntervalInMs = batchIntervalInMs - timeSinceLastBatchProduce;
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception when checking batch task", e);
      }
      try {
        Thread.sleep(sleepIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // reset interrupt flag
        break; // exit loop on interrupt
      } catch (Exception e) {
        LOGGER.error("Caught exception when waiting for next check", e);
      }
    }
  }

  void checkAndMaybeProduceBatchRecord() {
    getLock().lock();
    try {
      if (getBufferRecordList().isEmpty()) {
        return;
      }
      // A simple trick to make sure each time batch produce timestamp is different from previous one.
      if (System.currentTimeMillis() == lastBatchProduceMs) {
        Utils.sleep(1);
      }
      for (ProducerBufferRecord record: getBufferRecordList()) {
        if (record.shouldSkipProduce()) {
          ProducerBufferRecord latestRecord = getBufferRecordIndex().get(ByteBuffer.wrap(record.getSerializedKey()));
          if (latestRecord != null) {
            latestRecord.addDependentCallback(record.getCallback());
            latestRecord.addRecordToDependentRecordList(record);
          }
          continue;
        }
        if (record.getMessageType().equals(MessageType.UPDATE) && !record.getDependentRecordList().isEmpty()) {
          maybeUpdateRecordUpdatePayload(record);
        }
        try {
          sendRecord(record);
        } catch (Exception e) {
          record.getCallback().onCompletion(null, e);
        }
      }
    } finally {
      lastBatchProduceMs = System.currentTimeMillis();
      // In any case, state should be reset after produce.
      getBufferRecordIndex().clear();
      getBufferRecordList().clear();
      bufferSizeInBytes = 0;
      getLock().unlock();
    }
  }

  CompletableFuture<PubSubProduceResult> addRecordToBuffer(
      MessageType messageType,
      K key,
      V value,
      U update,
      int schemaId,
      int protocolId,
      PubSubProducerCallback callback,
      long logicalTimestamp) {
    maybeStartCheckExecutor();
    if (schemaId > 0) {
      getStoreSchemaCache().maybeUpdateSupersetSchema(schemaId);
    }
    byte[] serializedKey = getKeySerializer().serialize(getTopicName(), key);
    byte[] serializedValue = value == null ? null : getValueSerializer().serialize(getTopicName(), value);
    byte[] serializedUpdate = update == null ? null : getUpdateSerializer().serialize(getTopicName(), update);
    CompletableFuture<PubSubProduceResult> produceResultFuture;
    getLock().lock();
    try {
      // For logical timestamp record, timestamp compaction is not supported
      ProducerBufferRecord record;
      if (logicalTimestamp > 0) {
        record = new ProducerBufferRecord(
            messageType,
            serializedKey,
            serializedValue,
            serializedUpdate,
            schemaId,
            protocolId,
            callback,
            logicalTimestamp);
        produceResultFuture = new CompletableFuture<>();
      } else {
        record = new ProducerBufferRecord(
            messageType,
            serializedKey,
            serializedValue,
            serializedUpdate,
            schemaId,
            protocolId,
            callback,
            logicalTimestamp);
        ProducerBufferRecord prevRecord = getBufferRecordIndex().put(ByteBuffer.wrap(serializedKey), record);
        if (prevRecord != null) {
          prevRecord.setSkipProduce(true);
          // Try to reuse the same produce future.
          produceResultFuture = prevRecord.getProduceResultFuture();
        } else {
          produceResultFuture = new CompletableFuture<>();
        }
      }
      bufferSizeInBytes += record.getHeapSize();
      record.setProduceResultFuture(produceResultFuture);
      getBufferRecordList().add(record);
      // Make sure memory usage is under control
      if (getBufferSizeInBytes() >= getMaxBatchSizeInBytes()) {
        checkAndMaybeProduceBatchRecord();
      }
    } finally {
      getLock().unlock();
    }
    return produceResultFuture;
  }

  void sendRecord(ProducerBufferRecord record) {
    MessageType messageType = record.getMessageType();
    PubSubProducerCallback finalCallback = record.getCallback();
    if (!record.getDependentCallbackList().isEmpty()) {
      finalCallback = new ChainedPubSubCallback(record.getCallback(), record.getDependentCallbackList());
    }
    CompletableFuture<PubSubProduceResult> produceFuture = null;
    switch (messageType) {
      case PUT:
        produceFuture = getVeniceWriter().put(
            record.getSerializedKey(),
            record.getSerializedValue(),
            record.getSchemaId(),
            record.getTimestamp(),
            finalCallback);
        break;
      case UPDATE:
        produceFuture = getVeniceWriter().update(
            record.getSerializedKey(),
            record.getSerializedUpdate(),
            record.getSchemaId(),
            record.getProtocolId(),
            finalCallback,
            record.getTimestamp());
        break;
      case DELETE:
        produceFuture = getVeniceWriter().delete(record.getSerializedKey(), record.getTimestamp(), finalCallback);
        break;
      default:
        break;
    }
    // Chain up the produce future.
    if (produceFuture != null) {
      produceFuture.whenComplete((result, ex) -> {
        if (ex != null) {
          record.getProduceResultFuture().completeExceptionally(ex);
        } else {
          record.getProduceResultFuture().complete(result);
        }
      });
    }
  }

  /**
   * This method will only be invoked when the last message associated to a key is an UPDATE message.
   * The idea of this method is to locate the last PUT / DELETE message and start merging all the follow-up UPDATE
   * messages. All the messages before the last PUT / DELETE will be ignored as they will be fully overwritten.
   */
  void maybeUpdateRecordUpdatePayload(ProducerBufferRecord producerBufferRecord) {
    List<ProducerBufferRecord> dependentRecordList = producerBufferRecord.getDependentRecordList();
    // Try to locate the last non-UPDATE message's index in the dependent record list.
    int idx = dependentRecordList.size() - 1;
    while (idx >= 0) {
      if (!dependentRecordList.get(idx).getMessageType().equals(MessageType.UPDATE)) {
        break;
      }
      idx--;
    }
    GenericRecord resultUpdateRecord = null;
    ProducerBufferRecord anchorPutRecord;
    /**
     * If there is such message and if it is PUT message, we will deserialize it with superset value schema and convert
     * it to an UPDATE message and merge it will all the follow-up UPDATE message.
     * The reason to convert PUT message into UPDATE message instead of merging all the UPDATE messages into a PUT message
     * is: If there is no PUT message in the beginning, then using {@link WriteComputeHandlerV1} to build a default
     * value record and merge following UDPATE into PUT will change the semantic: It will set untouched fields back to
     * default value which is not a correct behavior.
     * Using superset schema is always correct for partial update enabled store.
     */
    int supersetSchemaId = getStoreSchemaCache().getLatestOrSupersetSchemaId();
    Schema supersetSchema = getStoreSchemaCache().getSupersetSchema();
    if (idx >= 0) {
      anchorPutRecord = dependentRecordList.get(idx);
      if (anchorPutRecord.getMessageType().equals(MessageType.PUT)) {
        resultUpdateRecord = convertValueRecordToUpdateRecord(
            getStoreSchemaCache().getUpdateSchema(),
            getValueDeserializer(anchorPutRecord.getSchemaId(), supersetSchemaId)
                .deserialize(anchorPutRecord.getSerializedValue()));
      }
    }
    /**
     * Sequentially merge all the follow-up UPDATE payload using {@link com.linkedin.venice.schema.writecompute.WriteComputeHandler}.
     */
    GenericRecord updateRecord;
    for (int i = idx + 1; i < dependentRecordList.size(); i++) {
      updateRecord = deserializeUpdateBytes(dependentRecordList.get(i).getSerializedUpdate());
      resultUpdateRecord = getUpdateHandler().mergeUpdateRecord(supersetSchema, resultUpdateRecord, updateRecord);
    }
    /**
     * Merge with the final UPDATE message.
     */
    updateRecord = deserializeUpdateBytes(producerBufferRecord.getSerializedUpdate());
    resultUpdateRecord = getUpdateHandler().mergeUpdateRecord(supersetSchema, resultUpdateRecord, updateRecord);
    /**
     * Re-serialize the payload and update the final to-be-produced record.
     */
    producerBufferRecord.updateSerializedUpdate(serializeMergedValueRecord(resultUpdateRecord));
  }

  private GenericRecord deserializeUpdateBytes(byte[] updateBytes) {
    // Use latest superset schema's update schema as both reader/writer schema to deserialize payload.
    Schema updateSchema = getStoreSchemaCache().getUpdateSchema();
    try {
      RecordDeserializer<GenericRecord> deserializer =
          getDeserializerCacheForUpdateValue().computeIfAbsent(updateSchema, k -> new VeniceConcurrentHashMap<>())
              .computeIfAbsent(
                  updateSchema,
                  k -> FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(updateSchema, updateSchema));
      return deserializer.deserialize(updateBytes);
    } catch (Exception e) {
      LOGGER.error(
          "Unable to deserialize update payload with update schema: {} associated with superset schema id: {}",
          updateSchema,
          getStoreSchemaCache().getLatestOrSupersetSchemaId());
      throw e;
    }
  }

  private byte[] serializeMergedValueRecord(GenericRecord mergedUpdateRecord) {
    Schema updateSchema = getStoreSchemaCache().getUpdateSchema();
    try {
      RecordSerializer serializer = getUpdateSerializerMap().computeIfAbsent(
          updateSchema,
          ignored -> FastSerializerDeserializerFactory.getFastAvroGenericSerializer(updateSchema));
      return serializer.serialize(mergedUpdateRecord);
    } catch (Exception e) {
      LOGGER.error(
          "Unable to serialize merged UPDATE payload with update schema: {} associated with superset schema id: {}",
          updateSchema,
          getStoreSchemaCache().getLatestOrSupersetSchemaId());
      throw e;
    }
  }

  void maybeStartCheckExecutor() {
    // Start the service only once
    if (isRunning.compareAndSet(false, true)) {
      checkServiceExecutor.execute(this::periodicCheckTask);
      lastBatchProduceMs = System.currentTimeMillis();
    }
  }

  GenericRecord convertValueRecordToUpdateRecord(Schema updateSchema, GenericRecord valueRecord) {
    UpdateBuilder builder = new UpdateBuilderImpl(updateSchema);
    for (Schema.Field valueField: valueRecord.getSchema().getFields()) {
      final String valueFieldName = valueField.name();
      builder.setNewFieldValue(valueFieldName, valueRecord.get(valueFieldName));
    }
    return builder.build();
  }

  List<ProducerBufferRecord> getBufferRecordList() {
    return bufferRecordList;
  }

  Map<ByteBuffer, ProducerBufferRecord> getBufferRecordIndex() {
    return bufferRecordIndex;
  }

  ReentrantLock getLock() {
    return lock;
  }

  int getMaxBatchSizeInBytes() {
    return maxBatchSizeInBytes;
  }

  int getBufferSizeInBytes() {
    return bufferSizeInBytes;
  }

  VeniceKafkaSerializer getKeySerializer() {
    return keySerializer;
  }

  VeniceKafkaSerializer getValueSerializer() {
    return valueSerializer;
  }

  VeniceKafkaSerializer getUpdateSerializer() {
    return updateSerializer;
  }

  VeniceWriter<byte[], byte[], byte[]> getVeniceWriter() {
    return veniceWriter;
  }

  BiIntKeyCache<RecordDeserializer<GenericRecord>> getDeserializerCacheForFullValue() {
    return deserializerCacheForFullValue;
  }

  Map<Schema, RecordSerializer<GenericRecord>> getUpdateSerializerMap() {
    return updateSerializerMap;
  }

  Map<Schema, Map<Schema, RecordDeserializer<GenericRecord>>> getDeserializerCacheForUpdateValue() {
    return deserializerCacheForUpdateValue;
  }

  RecordDeserializer<GenericRecord> getValueDeserializer(int readerSchemaId, int writerSchemaId) {
    return getDeserializerCacheForFullValue().get(readerSchemaId, writerSchemaId);
  }

  WriteComputeHandlerV1 getUpdateHandler() {
    return updateHandler;
  }

  SchemaFetcherBackedStoreSchemaCache getStoreSchemaCache() {
    return storeSchemaCache;
  }
}
