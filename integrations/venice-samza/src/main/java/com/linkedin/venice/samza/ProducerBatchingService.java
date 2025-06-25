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
  private final int maxBatchSize;
  private final ScheduledExecutorService checkServiceExecutor;
  private final List<ProducerBufferRecord> bufferRecordList = new ArrayList<>();
  private final Map<byte[], ProducerBufferRecord> bufferRecordIndex = new VeniceConcurrentHashMap<>();
  private volatile long lastBatchProduceMs;
  private volatile boolean isRunning = false;

  public ProducerBatchingService(VeniceWriter writer, long batchIntervalInMs, int maxBatchSize) {
    this.writer = writer;
    this.batchIntervalInMs = batchIntervalInMs;
    this.maxBatchSize = maxBatchSize;
    this.checkServiceExecutor = Executors.newScheduledThreadPool(1);
  }

  public void start() {
    isRunning = true;
    lastBatchProduceMs = System.currentTimeMillis();
    checkServiceExecutor.execute(this::periodicCheckTask);
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
      CompletableFuture<Void> future,
      long logicalTimestamp) {
    getLock().lock();
    try {
      /**
       * For logical timestamp record, timestamp compaction is not supported
       * For UPDATE, it is not supported in the current scope, support will be added in the future iteration.
       */
      if (messageType.equals(MessageType.UPDATE) || logicalTimestamp > 0) {
        getBufferRecordList().add(
            new ProducerBufferRecord(
                messageType,
                keyBytes,
                valueBytes,
                schemaId,
                protocolId,
                future,
                logicalTimestamp));
        return;
      }
      ProducerBufferRecord record =
          new ProducerBufferRecord(messageType, keyBytes, valueBytes, schemaId, protocolId, future, logicalTimestamp);
      getBufferRecordList().add(record);

      ProducerBufferRecord prevRecord = getBufferRecordIndex().put(keyBytes, record);
      LOGGER.info("DEBUGGING: {}, {}, {}", prevRecord, keyBytes, record);
      if (prevRecord != null) {
        prevRecord.setSkipProduce(true);
      }
      // Make sure memory usage is under control
      if (getBufferRecordList().size() >= getMaxBatchSize()) {
        LOGGER.info("DEBUGGING");
        checkAndMaybeProduceBatchRecord();
      }
    } finally {
      getLock().unlock();
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
      int protocolId,
      CompletableFuture<Void> future) {
    addRecordToBuffer(
        messageType,
        keyBytes,
        valueBytes,
        schemaId,
        protocolId,
        future,
        VeniceWriter.APP_DEFAULT_LOGICAL_TS);
  }

  void checkAndMaybeProduceBatchRecord() {
    getLock().lock();
    try {
      if (getBufferRecordList().isEmpty()) {
        return;
      }
      for (ProducerBufferRecord record: getBufferRecordList()) {
        if (record.shouldSkipProduce()) {
          ProducerBufferRecord latestRecord = getBufferRecordIndex().get(record.getKeyBytes());
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
      lastBatchProduceMs = System.currentTimeMillis();
      // In any case, state should be reset after produce.
      getBufferRecordIndex().clear();
      getBufferRecordList().clear();
      getLock().unlock();
    }
  }

  void sendRecord(ProducerBufferRecord record) {
    MessageType messageType = record.getMessageType();
    CompletableFutureCallback callback = new CompletableFutureCallback(record.getFuture());
    callback.setDependentFutureList(record.getDependentFutureList());
    switch (messageType) {
      case PUT:
        getWriter().put(
            record.getKeyBytes(),
            record.getValueBytes(),
            record.getSchemaId(),
            record.getLogicalTimestamp(),
            callback);
        break;
      case UPDATE:
        getWriter().update(
            record.getKeyBytes(),
            record.getValueBytes(),
            record.getSchemaId(),
            record.getProtocolId(),
            callback,
            record.getLogicalTimestamp());
        break;
      case DELETE:
        getWriter().delete(record.getKeyBytes(), record.getLogicalTimestamp(), callback);
        break;
      default:
        break;
    }
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

  VeniceWriter getWriter() {
    return writer;
  }

  public List<ProducerBufferRecord> getBufferRecordList() {
    return bufferRecordList;
  }

  public Map<byte[], ProducerBufferRecord> getBufferRecordIndex() {
    return bufferRecordIndex;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public long getBatchIntervalInMs() {
    return batchIntervalInMs;
  }

  public ReentrantLock getLock() {
    return lock;
  }
}
