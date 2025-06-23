package com.linkedin.venice.samza;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.Closeable;
import java.io.IOException;
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
 * This class serves as a simple batch produce service inside {@link VeniceSystemProducer}.
 * For PUT / DELETE, only the last one in the batch will be produced to {@link VeniceWriter}.
 * For UPDATE, the batching support will be added in the future.
 * For message with logical timestamp, the batching is not supported.
 */
public class ProducerBatchingService implements Closeable {
  public static final Logger LOGGER = LogManager.getLogger(ProducerBatchingService.class);
  private final VeniceWriter writer;
  private final ReentrantLock lock = new ReentrantLock();
  private final long batchIntervalInMs;
  private final ScheduledExecutorService checkServiceExecutor;
  private final List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
  private final Map<byte[], ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
  private volatile boolean isRunning = false;

  public ProducerBatchingService(VeniceWriter writer, long batchIntervalInMs) {
    this.writer = writer;
    this.batchIntervalInMs = batchIntervalInMs;
    this.checkServiceExecutor = Executors.newScheduledThreadPool(1);
  }

  public void start() {
    isRunning = true;
    checkServiceExecutor.scheduleWithFixedDelay(this::checkBatchTask, 0, batchIntervalInMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws IOException {
    isRunning = false;
    checkServiceExecutor.shutdown();
    try {
      if (!checkServiceExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        checkServiceExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }

  /**
   * Add a new message into buffer with logical timestamp specified.
   */
  public void addRecordToBuffer(
      MessageType messageType,
      byte[] keyBytes,
      byte[] valueBytes,
      int schemaId,
      int protocolId,
      long logicalTimestamp) {
    lock.lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       * For UPDATE, it is not supported in the current scope, support will be added in the future iteration.
       */
      if (messageType.equals(MessageType.UPDATE) || logicalTimestamp != -1) {
        bufferRecordList
            .add(new ProducerBufferRecord(messageType, keyBytes, valueBytes, schemaId, protocolId, logicalTimestamp));
        return;
      }
      ProducerBufferRecord record =
          new ProducerBufferRecord(messageType, keyBytes, valueBytes, schemaId, protocolId, logicalTimestamp);
      bufferRecordList.add(record);

      ProducerBufferRecord prevRecord = bufferRecordIndex.putIfAbsent(keyBytes, record);
      if (prevRecord != null) {
        prevRecord.setSkipProduce(true);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Add a new message into buffer without logical timestamp specified.
   */
  public void addRecordToBuffer(
      MessageType messageType,
      byte[] keyBytes,
      byte[] valueBytes,
      int schemaId,
      int protocolId) {
    addRecordToBuffer(messageType, keyBytes, valueBytes, schemaId, protocolId, VeniceWriter.APP_DEFAULT_LOGICAL_TS);
  }

  void checkAndMaybeProduceBatchRecord() {
    if (bufferRecordList.isEmpty()) {
      return;
    }
    lock.lock();
    try {
      for (ProducerBufferRecord record: bufferRecordList) {
        if (record.shouldSkipProduce()) {
          ProducerBufferRecord latestRecord = bufferRecordIndex.get(record.getKeyBytes());
          if (latestRecord != null) {
            latestRecord.addFutureToDependentFutureList(record.getFuture());
          }
          continue;
        }
        try {
          sendRecord(record);
        } catch (Exception e) {
          record.getFuture().completeExceptionally(e);
          for (CompletableFuture<Void> dependentFuture: record.getDependentFutureList()) {
            dependentFuture.completeExceptionally(e);
          }
        }
      }
    } finally {
      // In any case, state should be reset after produce.
      bufferRecordList.clear();
      bufferRecordIndex.clear();
      lock.unlock();
    }
  }

  void sendRecord(ProducerBufferRecord record) {
    MessageType messageType = record.getMessageType();
    CompletableFutureCallback callback = new CompletableFutureCallback(record.getFuture());
    callback.setDependentFutureList(record.getDependentFutureList());
    switch (messageType) {
      case PUT:
        writer.put(
            record.getKeyBytes(),
            record.getValueBytes(),
            record.getSchemaId(),
            record.getLogicalTimestamp(),
            callback);
        break;
      case UPDATE:
        writer.update(
            record.getKeyBytes(),
            record.getValueBytes(),
            record.getSchemaId(),
            record.getProtocolId(),
            callback,
            record.getLogicalTimestamp());
        break;
      case DELETE:
        writer.delete(record.getKeyBytes(), record.getLogicalTimestamp(), callback);
        break;
      default:
        break;
    }
  }

  void checkBatchTask() {
    if (isRunning) {
      try {
        checkAndMaybeProduceBatchRecord();
      } catch (Exception e) {
        LOGGER.error("Caught exception when checking batch task", e);
      }
    }
  }
}
