package com.linkedin.venice.writer;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a batching implementation of {@link VeniceWriter}. There are two configs that control the batching
 * behavior:
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
public class BatchingVeniceWriter<K, V, U> extends VeniceWriter<K, V, U> {
  public static final Logger LOGGER = LogManager.getLogger(BatchingVeniceWriter.class);

  private final long batchIntervalInMs;
  private final int maxBatchSizeInBytes;
  private final ReentrantLock lock = new ReentrantLock();
  private final ScheduledExecutorService checkServiceExecutor;
  private final List<ProducerBufferRecord<V, U>> bufferRecordList = new ArrayList<>();
  private final Map<ByteBuffer, ProducerBufferRecord<V, U>> bufferRecordIndex;
  private final VeniceWriter<byte[], V, U> veniceWriter;
  private final VeniceKafkaSerializer keySerializer;
  private volatile long lastBatchProduceMs;
  private volatile boolean isRunning;
  private int bufferSizeInBytes;

  public BatchingVeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter) {
    super(params, props, producerAdapter);
    this.batchIntervalInMs = params.getBatchIntervalInMs();
    this.maxBatchSizeInBytes = params.getMaxBatchSizeInBytes();
    this.bufferRecordIndex = new VeniceConcurrentHashMap<>();
    this.checkServiceExecutor = Executors.newScheduledThreadPool(1);
    this.keySerializer = params.getKeyPayloadSerializer();
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
      PubSubProducerCallback callback,
      long logicalTs) {
    return addRecordToBuffer(
        MessageType.UPDATE,
        key,
        null,
        update,
        valueSchemaId,
        derivedSchemaId,
        callback,
        logicalTs);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    return update(key, update, valueSchemaId, derivedSchemaId, callback, APP_DEFAULT_LOGICAL_TS);
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
    super.close(gracefulClose);
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
    CompletableFuture<PubSubProduceResult> result;
    getLock().lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       * For UPDATE, it is not supported in the current scope, support will be added in the future iteration.
       */
      if (messageType.equals(MessageType.UPDATE) || logicalTimestamp > 0) {
        ProducerBufferRecord<V, U> record = new ProducerBufferRecord<>(
            messageType,
            serializedKey,
            value,
            update,
            schemaId,
            protocolId,
            callback,
            logicalTimestamp);
        CompletableFuture<PubSubProduceResult> produceFuture = new CompletableFuture<>();
        record.setProduceResultFuture(produceFuture);
        getBufferRecordList().add(record);
        return produceFuture;
      }
      ProducerBufferRecord<V, U> record = new ProducerBufferRecord<>(
          messageType,
          serializedKey,
          value,
          update,
          schemaId,
          protocolId,
          callback,
          logicalTimestamp);
      bufferSizeInBytes += record.getHeapSize();
      getBufferRecordList().add(record);
      ProducerBufferRecord<V, U> prevRecord = getBufferRecordIndex().put(ByteBuffer.wrap(serializedKey), record);
      result = null;
      if (prevRecord != null) {
        prevRecord.setSkipProduce(true);
        result = prevRecord.getProduceResultFuture();
      }
      // Try to reuse the same produce future.
      if (result == null) {
        result = new CompletableFuture<>();
        if (prevRecord != null) {
          prevRecord.setProduceResultFuture(result);
        }
      }
      record.setProduceResultFuture(result);

      // Make sure memory usage is under control
      if (getBufferSizeInBytes() >= getMaxBatchSizeInBytes()) {
        checkAndMaybeProduceBatchRecord();
      }
    } finally {
      getLock().unlock();
    }
    return result;
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
