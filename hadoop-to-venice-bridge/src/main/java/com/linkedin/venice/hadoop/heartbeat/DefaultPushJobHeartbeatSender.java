package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

@NotThreadSafe
class DefaultPushJobHeartbeatSender implements PushJobHeartbeatSender {
  private static final Logger LOGGER = Logger.getLogger(DefaultPushJobHeartbeatSender.class);
  private static final Duration DEFAULT_SEND_CALLBACK_AWAIT_TIMEOUT = Duration.ofSeconds(10);

  private final Duration interval;
  private final Duration initialDelay;
  private boolean running;
  private final ScheduledExecutorService executorService;
  private final VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private final int valueSchemaId;
  private final String heartbeatKafkaTopicName;
  private final VeniceAvroKafkaSerializer keySerializer;
  private final VeniceAvroKafkaSerializer valueSerializer;

  private String storeName;
  private int storeVersion;
  private Instant heartbeatStartTime;
  private long successfulHeartbeatCount;
  private long failedHeartbeatCount;
  private Exception firstSendHeartbeatException;

  DefaultPushJobHeartbeatSender(
          @Nonnull Duration initialDelay,
          @Nonnull Duration interval,
          @Nonnull VeniceWriter<byte[], byte[], byte[]> veniceWriter,
          @Nonnull Schema heartbeatKeySchema,
          @Nonnull Map<Integer, Schema> valueSchemasById,
          @Nonnull String heartbeatKafkaTopicName
  ) {
    this.initialDelay = Utils.notNull(initialDelay);
    this.interval = Utils.notNull(interval);
    this.veniceWriter = Utils.notNull(veniceWriter);
    validateSchemasMatch(BatchJobHeartbeatKey.SCHEMA$, heartbeatKeySchema);
    // Expect one of the given value schemas to match with the current value schema
    this.valueSchemaId = getSchemaIdForSchemaOrFail(BatchJobHeartbeatValue.SCHEMA$, valueSchemasById);
    this.heartbeatKafkaTopicName = Utils.stringNotNullNorEmpty(heartbeatKafkaTopicName);
    this.keySerializer = new VeniceAvroKafkaSerializer(heartbeatKeySchema);
    this.valueSerializer = new VeniceAvroKafkaSerializer(BatchJobHeartbeatValue.SCHEMA$);
    this.running = false;
    this.executorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("push-job-heartbeat-thread"));
    this.successfulHeartbeatCount = 0;
    this.failedHeartbeatCount = 0;
    this.firstSendHeartbeatException = null;
  }

  private int getSchemaIdForSchemaOrFail(Schema expectedSchema, Map<Integer, Schema> valueSchemasById) {
    for (Map.Entry<Integer, Schema> schemaIdAndSchema : valueSchemasById.entrySet()) {
      if (Objects.equals(expectedSchema, schemaIdAndSchema.getValue())) {
        return schemaIdAndSchema.getKey();
      }
    }
    throw new IllegalArgumentException("No schema %s found in valueSchemasById %s");
  }

  private void validateSchemasMatch(Schema expectedSchema, Schema actualSchema) {
    if (!Objects.equals(expectedSchema, actualSchema)) {
      throw new IllegalArgumentException(String.format("Expected schema %s and actual schema %s",
              expectedSchema.toString(), actualSchema.toString()));
    }
  }

  @Override
  public void start(
      @Nonnull String storeName,
      int storeVersion
  ) {
    if (running) {
      LOGGER.warn("Already started");
      return;
    }
    running = true;
    this.storeName = Utils.notNull(storeName);
    this.storeVersion = storeVersion;
    this.heartbeatStartTime = Instant.now();
    LOGGER.info(String.format("Start sending liveness heartbeats for [store=%s, version=%s] with initial delay %d ms and " +
            "interval %d ms...", this.storeName, this.storeVersion, this.initialDelay.toMillis(), this.interval.toMillis()));
    executorService.scheduleAtFixedRate(this, initialDelay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    if (!running) {
      LOGGER.warn("Already stopped or never started");
      return;
    }
    running = false;
    executorService.shutdown();
    // Send one last heartbeat that marks the end of this heartbeat session
    // TODO (lcli): consider making this timeout configurable
    LOGGER.info("Sending last heartbeat...");
    sendHeartbeat(createHeartbeatKey(), createHeartbeatValue(), DEFAULT_SEND_CALLBACK_AWAIT_TIMEOUT, true);
    LOGGER.info("Closing the heartbeat VeniceWriter");
    veniceWriter.close();
    LOGGER.info(String.format("Liveness heartbeat stopped for [store=%s, version=%s] " +
            "with %d successful heartbeat(s) and %d failed heartbeat(s) and in total took %d second(s)",
            this.storeName, this.storeVersion, successfulHeartbeatCount, failedHeartbeatCount,
            Duration.between(this.heartbeatStartTime, Instant.now()).getSeconds())
    );
  }

  @Override
  @Nonnull
  public Duration getHeartbeatSendInterval() {
    return interval;
  }

  @Override
  @Nonnull
  public Duration getHeartbeatInitialDelay() {
    return initialDelay;
  }

  @Override
  public void run() {
    sendHeartbeat(createHeartbeatKey(), createHeartbeatValue(), DEFAULT_SEND_CALLBACK_AWAIT_TIMEOUT, false);
  }

  @Override
  public Optional<Exception> getFirstSendHeartbeatException() {
   return Optional.ofNullable(firstSendHeartbeatException);
  }

  private BatchJobHeartbeatKey createHeartbeatKey() {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeName = this.storeName;
    batchJobHeartbeatKey.storeVersion = this.storeVersion;
    return batchJobHeartbeatKey;
  }

  private BatchJobHeartbeatValue createHeartbeatValue() {
    BatchJobHeartbeatValue batchJobHeartbeatValue = new BatchJobHeartbeatValue();
    batchJobHeartbeatValue.timestamp = System.currentTimeMillis();
    return batchJobHeartbeatValue;
  }

  private void sendHeartbeat(
          BatchJobHeartbeatKey batchJobHeartbeatKey,
          BatchJobHeartbeatValue BatchJobHeartbeatValue,
          Duration sendTimeout,
          boolean isLastHeartbeat
  ) {
    byte[] keyBytes = keySerializer.serialize(heartbeatKafkaTopicName, batchJobHeartbeatKey);
    byte[] valueBytes = valueSerializer.serialize(heartbeatKafkaTopicName, BatchJobHeartbeatValue);
    CountDownLatch sendComplete = new CountDownLatch(1);
    final Instant sendStartTime = Instant.now();
    final Callback callback = (RecordMetadata metadata, Exception exception) -> {
      Duration sendDuration = Duration.between(sendStartTime, Instant.now());
      if (exception == null) {
        successfulHeartbeatCount++;
        LOGGER.info("Sending one heartbeat event successfully. Took: " + sendDuration.toMillis() + " ms");
      } else {
        failedHeartbeatCount++;
        if (firstSendHeartbeatException == null) {
          firstSendHeartbeatException = exception;
        }
        LOGGER.info("Failed to send one heartbeat event after " + sendDuration.toMillis() + " ms", exception);
      }
      sendComplete.countDown();
    };

    if (isLastHeartbeat) {
      veniceWriter.delete(keyBytes, callback);
    } else {
      veniceWriter.put(keyBytes, valueBytes, valueSchemaId, callback);
    }
    veniceWriter.flush();

    try {
      if (!sendComplete.await(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        LOGGER.warn("Liveness heartbeat sent did not get ack-ed by remote server after " + sendTimeout.toMillis() + " ms");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Liveness heartbeat sent was interrupted", e);
    }
  }
}
