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
 * For messages within the same batch, only the last one will be produced into the topic, except for those mentioned below.
 * (1) UPDATE message: It will be supported in the future.
 * (2) Message with logical timestamp: It will be sent out individually.
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
  private final Map<Schema, RecordSerializer<GenericRecord>> valueSerializerMap = new VeniceConcurrentHashMap<>();

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
      storeSchemaCache.maybeUpdateSupersetSchema(schemaId);
    }
    byte[] serializedKey = getKeySerializer().serialize(getTopicName(), key);
    LOGGER.info("DEBUGGING: INCOMING KEY: {}, MSG: {}", serializedKey, messageType);
    byte[] serializedValue = value == null ? null : getValueSerializer().serialize(getTopicName(), value);
    byte[] serializedUpdate = update == null ? null : getUpdateSerializer().serialize(getTopicName(), update);
    CompletableFuture<PubSubProduceResult> produceResultFuture;
    getLock().lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       */
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
            record.getLogicalTimestamp(),
            finalCallback);
        break;
      case UPDATE:
        produceFuture = getVeniceWriter().update(
            record.getSerializedKey(),
            record.getSerializedUpdate(),
            record.getSchemaId(),
            record.getProtocolId(),
            finalCallback,
            record.getLogicalTimestamp());
        break;
      case DELETE:
        produceFuture =
            getVeniceWriter().delete(record.getSerializedKey(), record.getLogicalTimestamp(), finalCallback);
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

  void maybeUpdateRecordUpdatePayload(ProducerBufferRecord producerBufferRecord) {
    List<ProducerBufferRecord> dependentRecordList = producerBufferRecord.getDependentRecordList();
    int idx = dependentRecordList.size() - 1;
    while (idx >= 0) {
      if (!dependentRecordList.get(idx).getMessageType().equals(MessageType.UPDATE)) {
        break;
      }
      idx--;
    }
    GenericRecord valueRecord = null;
    ProducerBufferRecord anchorRecord = null;
    if (idx >= 0) {
      anchorRecord = dependentRecordList.get(idx);
      if (anchorRecord.getMessageType().equals(MessageType.PUT)) {
        valueRecord = deserializerCacheForFullValue
            .get(anchorRecord.getSchemaId(), storeSchemaCache.getLatestOrSupersetSchemaId())
            .deserialize(anchorRecord.getSerializedValue());
      }
    }
    Schema supersetSchema = storeSchemaCache.getSupersetSchema();
    GenericRecord updateRecord;
    for (int i = idx + 1; i < dependentRecordList.size(); i++) {
      updateRecord = deserializeUpdateBytes(dependentRecordList.get(i).getSerializedUpdate());
      valueRecord = updateHandler.updateValueRecord(supersetSchema, valueRecord, updateRecord);
    }
    updateRecord = deserializeUpdateBytes(producerBufferRecord.getSerializedUpdate());
    valueRecord = updateHandler.updateValueRecord(supersetSchema, valueRecord, updateRecord);

    producerBufferRecord.updateSerializedValue(serializeMergedValueRecord(valueRecord));
  }

  private GenericRecord deserializeUpdateBytes(byte[] updateBytes) {
    Schema writerSchema = storeSchemaCache.getUpdateSchema();
    Schema readerSchema = writerSchema;
    RecordDeserializer<GenericRecord> deserializer =
        deserializerCacheForUpdateValue.computeIfAbsent(writerSchema, k -> new VeniceConcurrentHashMap<>())
            .computeIfAbsent(
                readerSchema,
                k -> FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema));
    return deserializer.deserialize(updateBytes);
  }

  private byte[] serializeMergedValueRecord(GenericRecord mergedValue) {
    Schema schema = storeSchemaCache.getSupersetSchema();
    RecordSerializer serializer = valueSerializerMap
        .computeIfAbsent(schema, ignored -> FastSerializerDeserializerFactory.getFastAvroGenericSerializer(schema));
    return serializer.serialize(mergedValue);
  }

  void maybeStartCheckExecutor() {
    // Start the service only once
    if (isRunning.compareAndSet(false, true)) {
      checkServiceExecutor.execute(this::periodicCheckTask);
      lastBatchProduceMs = System.currentTimeMillis();
    }
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
}
