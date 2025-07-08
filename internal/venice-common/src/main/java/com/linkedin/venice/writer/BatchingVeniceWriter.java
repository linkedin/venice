package com.linkedin.venice.writer;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BatchingVeniceWriter<K, V, U> extends VeniceWriter<K, V, U> {
  public static final Logger LOGGER = LogManager.getLogger(BatchingVeniceWriter.class);

  private final long batchIntervalInMs;
  private final int maxBatchSizeInBytes;
  private final ReentrantLock lock = new ReentrantLock();
  private final ScheduledExecutorService checkServiceExecutor;
  private final List<ProducerBufferRecord<K, V, U>> bufferRecordList = new ArrayList<>();
  private final Map<K, ProducerBufferRecord<K, V, U>> bufferRecordIndex;
  private final Comparator<K> keyComparator;
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
    this.keyComparator = params.getKeyComparator();
    this.bufferRecordIndex = new TreeMap<>(keyComparator);
    this.checkServiceExecutor = Executors.newScheduledThreadPool(1);
    isRunning = true;
    lastBatchProduceMs = System.currentTimeMillis();
    checkServiceExecutor.execute(this::periodicCheckTask);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(K key, long logicalTs, PubSubProducerCallback callback) {
    addRecordToBuffer(MessageType.DELETE, key, null, null, -1, -1, callback, logicalTs);
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
    addRecordToBuffer(MessageType.PUT, key, value, null, valueSchemaId, -1, callback, logicalTs);
    // TODO: Add the link later;
    return null;
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
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback,
      long logicalTs) {
    addRecordToBuffer(MessageType.UPDATE, key, null, update, valueSchemaId, derivedSchemaId, callback, logicalTs);
    return null;
  }

  @Override
  public Future<PubSubProduceResult> update(
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

  void periodicCheckTask() {
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
      for (ProducerBufferRecord<K, V, U> record: getBufferRecordList()) {
        if (record.shouldSkipProduce()) {
          ProducerBufferRecord<K, V, U> latestRecord = getBufferRecordIndex().get(record.getKey());
          if (latestRecord != null) {
            LOGGER.info("DEBUGGING ADD DEP CALLBACK: {} {}", latestRecord, record);
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

  void addRecordToBuffer(
      MessageType messageType,
      K key,
      V value,
      U update,
      int schemaId,
      int protocolId,
      PubSubProducerCallback callback,
      long logicalTimestamp) {
    getLock().lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       * For UPDATE, it is not supported in the current scope, support will be added in the future iteration.
       */
      if (messageType.equals(MessageType.UPDATE) || logicalTimestamp > 0) {
        getBufferRecordList().add(
            new ProducerBufferRecord<>(
                messageType,
                key,
                value,
                update,
                schemaId,
                protocolId,
                callback,
                logicalTimestamp));
        return;
      }
      ProducerBufferRecord<K, V, U> record =
          new ProducerBufferRecord<>(messageType, key, value, update, schemaId, protocolId, callback, logicalTimestamp);
      bufferSizeInBytes += record.getHeapSize();
      getBufferRecordList().add(record);
      ProducerBufferRecord<K, V, U> prevRecord = getBufferRecordIndex().put(key, record);
      if (prevRecord != null) {
        prevRecord.setSkipProduce(true);
      }
      // Make sure memory usage is under control
      if (getBufferSizeInBytes() >= getMaxBatchSizeInBytes()) {
        checkAndMaybeProduceBatchRecord();
      }
    } finally {
      getLock().unlock();
    }
  }

  void sendRecord(ProducerBufferRecord<K, V, U> record) {
    MessageType messageType = record.getMessageType();
    PubSubProducerCallback finalCallback = record.getCallback();
    if (!record.getDependentCallbackList().isEmpty()) {
      LOGGER.info("DEBUGGING: CHAINED: {}, {}", record.getKey(), record.getDependentCallbackList().size());
      finalCallback = new ChainedPubSubCallback(record.getCallback(), record.getDependentCallbackList());
    } else {
      LOGGER.info("DEBUGGING: NOT CHAINED: {}, {}", record.getKey(), record.getDependentCallbackList().size());
    }
    switch (messageType) {
      case PUT:
        internalPut(
            record.getKey(),
            record.getValue(),
            record.getSchemaId(),
            record.getLogicalTimestamp(),
            finalCallback);
        break;
      case UPDATE:
        internalUpdate(
            record.getKey(),
            record.getUpdate(),
            record.getSchemaId(),
            record.getProtocolId(),
            record.getLogicalTimestamp(),
            finalCallback);
        break;
      case DELETE:
        internalDelete(record.getKey(), record.getLogicalTimestamp(), finalCallback);
        break;
      default:
        break;
    }
  }

  List<ProducerBufferRecord<K, V, U>> getBufferRecordList() {
    return bufferRecordList;
  }

  Map<K, ProducerBufferRecord<K, V, U>> getBufferRecordIndex() {
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

  Future<PubSubProduceResult> internalUpdate(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTs,
      PubSubProducerCallback callback) {
    return super.update(key, update, valueSchemaId, derivedSchemaId, callback, logicalTs);
  }

  Future<PubSubProduceResult> internalDelete(K key, long logicalTs, PubSubProducerCallback callback) {
    return super.delete(key, logicalTs, callback);
  }

  Future<PubSubProduceResult> internalPut(
      K key,
      V value,
      int valueSchemaId,
      long logicalTs,
      PubSubProducerCallback callback) {
    return super.put(key, value, valueSchemaId, logicalTs, callback);
  }

}
