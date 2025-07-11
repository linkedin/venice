package com.linkedin.venice.writer;

import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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
import java.util.concurrent.locks.ReentrantLock;
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
  private final List<ProducerBufferRecord<V, U>> bufferRecordList = new ArrayList<>();
  private final Map<ByteBuffer, ProducerBufferRecord<V, U>> bufferRecordIndex = new VeniceConcurrentHashMap<>();
  private final VeniceWriter<byte[], V, U> veniceWriter;
  private final VeniceKafkaSerializer keySerializer;
  private volatile long lastBatchProduceMs;
  private volatile boolean isRunning;
  private int bufferSizeInBytes;

  public BatchingVeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter) {
    super(params.getTopicName());
    this.batchIntervalInMs = params.getBatchIntervalInMs();
    this.maxBatchSizeInBytes = params.getMaxBatchSizeInBytes();
    this.keySerializer = params.getKeyPayloadSerializer();
    /**
     * We introduce an internal Venice writer with byte[] as the key type for any input key type. This is to make sure
     * internal buffer is indexed correctly when input key type is byte[]. For internal buffer index map, if key type is
     * byte[], comparison will be incorrect as byte[] is compared by reference, instead of value.
     * Since we will serialize key into byte[] in the internal buffer and later use it to produce with internal writer,
     * internal writer should only have the default No-Op key serializer. 
     */
    VeniceWriterOptions internalWriterParams =
        new VeniceWriterOptions.Builder(params.getTopicName(), params).setKeyPayloadSerializer(new DefaultSerializer())
            .build();
    this.veniceWriter = new VeniceWriter<>(internalWriterParams, props, producerAdapter);
    // Start the service.
    lastBatchProduceMs = System.currentTimeMillis();
    isRunning = true;
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
    return null;
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return null;
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    return null;
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
    isRunning = false;
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
    while (isRunning) {
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
      for (ProducerBufferRecord<V, U> record: getBufferRecordList()) {
        if (record.shouldSkipProduce()) {
          ProducerBufferRecord<V, U> latestRecord =
              getBufferRecordIndex().get(ByteBuffer.wrap(record.getSerializedKey()));
          if (latestRecord != null) {
            latestRecord.addDependentCallback(record.getCallback());
          }
          continue;
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
    byte[] serializedKey = getKeySerializer().serialize(getTopicName(), key);
    CompletableFuture<PubSubProduceResult> produceResultFuture;
    getLock().lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       * For UPDATE, it is not supported in the current scope, support will be added in the future iteration.
       */
      ProducerBufferRecord<V, U> record;
      if (messageType.equals(MessageType.UPDATE) || logicalTimestamp > 0) {
        record = new ProducerBufferRecord<>(
            messageType,
            serializedKey,
            value,
            update,
            schemaId,
            protocolId,
            callback,
            logicalTimestamp);
        produceResultFuture = new CompletableFuture<>();
      } else {
        record = new ProducerBufferRecord<>(
            messageType,
            serializedKey,
            value,
            update,
            schemaId,
            protocolId,
            callback,
            logicalTimestamp);
        ProducerBufferRecord<V, U> prevRecord = getBufferRecordIndex().put(ByteBuffer.wrap(serializedKey), record);
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

  void sendRecord(ProducerBufferRecord<V, U> record) {
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
            record.getValue(),
            record.getSchemaId(),
            record.getLogicalTimestamp(),
            finalCallback);
        break;
      case UPDATE:
        produceFuture = getVeniceWriter().update(
            record.getSerializedKey(),
            record.getUpdate(),
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

  void start() {
    checkServiceExecutor.execute(this::periodicCheckTask);
  }

  List<ProducerBufferRecord<V, U>> getBufferRecordList() {
    return bufferRecordList;
  }

  Map<ByteBuffer, ProducerBufferRecord<V, U>> getBufferRecordIndex() {
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

  VeniceWriter<byte[], V, U> getVeniceWriter() {
    return veniceWriter;
  }
}
