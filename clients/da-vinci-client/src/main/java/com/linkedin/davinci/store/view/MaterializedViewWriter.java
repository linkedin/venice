package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MaterializedViewWriter extends VeniceViewWriter {
  private final PubSubProducerAdapterFactory pubSubProducerAdapterFactory;
  private final MaterializedView internalView;
  private final ReentrantLock broadcastHBLock = new ReentrantLock();
  private final Map<Integer, Long> partitionToHeartbeatTimestampMap = new HashMap<>();
  private final Clock clock;
  private VeniceWriter veniceWriter;
  private String materializedViewTopicName;
  private long lastHBBroadcastTimestamp;

  /**
   * These configs can be exposed to view parameters if or server configs if needed
   */
  private static final long DEFAULT_HEARTBEAT_BROADCAST_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long DEFAULT_HEARTBEAT_BROADCAST_DELAY_THRESHOLD = TimeUnit.MINUTES.toMillis(5);
  private static final int DEFAULT_PARTITION_TO_ALWAYS_BROADCAST = 0;
  private static final Logger LOGGER = LogManager.getLogger(MaterializedViewWriter.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public MaterializedViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters,
      Clock clock) {
    super(props, store, keySchema, extraViewParameters);
    pubSubProducerAdapterFactory = props.getVeniceServerConfig().getPubSubClientsFactory().getProducerAdapterFactory();
    internalView = new MaterializedView(props.getCombinedProperties().toProperties(), store, extraViewParameters);
    this.clock = clock;
  }

  public MaterializedViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    this(props, store, keySchema, extraViewParameters, Clock.systemUTC());
  }

  @Override
  public CompletableFuture<PubSubProduceResult> processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      byte[] key,
      int version,
      int newValueSchemaId,
      int oldValueSchemaId,
      GenericRecord replicationMetadataRecord) {
    return processRecord(newValue, key, version, newValueSchemaId);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> processRecord(
      ByteBuffer newValue,
      byte[] key,
      int version,
      int newValueSchemaId) {
    if (veniceWriter == null) {
      initializeVeniceWriter(version);
    }
    return veniceWriter.put(key, newValue.array(), newValueSchemaId);
  }

  @Override
  public void processControlMessage(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {
    final ControlMessageType type = ControlMessageType.valueOf(controlMessage);
    // Ignore other control messages for materialized view.
    if (type == ControlMessageType.START_OF_SEGMENT && Arrays.equals(kafkaKey.getKey(), KafkaKey.HEART_BEAT.getKey())) {
      maybePropagateHeartbeatLowWatermarkToViewTopic(
          partition,
          partitionConsumptionState,
          kafkaMessageEnvelope.producerMetadata.messageTimestamp,
          version);
    }
  }

  @Override
  public String getWriterClassName() {
    return internalView.getWriterClassName();
  }

  // package private for testing purposes.
  VeniceWriterOptions buildWriterOptions(int version) {
    // We need to change this and have a map of writers if one materialized view will have many topics.
    materializedViewTopicName =
        internalView.getTopicNamesAndConfigsForVersion(version).keySet().stream().findAny().get();
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(materializedViewTopicName);
    Version storeVersionConfig = store.getVersionOrThrow(version);
    configBuilder.setPartitionCount(internalView.getViewPartitionCount());
    configBuilder.setChunkingEnabled(storeVersionConfig.isChunkingEnabled());
    configBuilder.setRmdChunkingEnabled(storeVersionConfig.isRmdChunkingEnabled());
    configBuilder.setPartitioner(internalView.getViewPartitioner());
    return setProducerOptimizations(configBuilder).build();
  }

  synchronized private void initializeVeniceWriter(int version) {
    if (veniceWriter == null) {
      veniceWriter = new VeniceWriterFactory(props, pubSubProducerAdapterFactory, null)
          .createVeniceWriter(buildWriterOptions(version));
    }
  }

  /**
   * View topic's partitioner and partition count could be different from the VT. In order to ensure we are capturing
   * all potential lag in the VT ingestion from the view topic, we will broadcast the low watermark observed from every
   * VT leader to all partitions of the view topic. To reduce the heartbeat spam we can use a strategy as follows:
   *    1. Leader of partition 0 always broadcasts its low watermark timestamp to all view topic partitions.
   *    2. Leader of other partitions will only broadcast its heartbeat low watermark timestamp if it's sufficiently
   *       stale. This is configurable but by default it could be >= 5 minutes. This is because broadcasting redundant
   *       up-to-date heartbeat in view topic is not meaningful when the main goal here is just to identify if there
   *       are any lagging partitions or the largest lag amongst all VT partitions. Since lag in any VT partition could
   *       result in lag in one or more view topic partitions.
   *    3. This broadcasting heartbeat mechanism will only provide lag info to view topic consumers if the corresponding
   *       VT consumption is not stuck. e.g. if one VT partition is stuck we won't be able to detect such issue from the
   *       view topic heartbeats because VT partitions that are not stuck will be broadcasting heartbeats. Due to this
   *       reason we can also clear and rebuild the partition to timestamp map to simplify the maintenance logic.
   */
  private void maybePropagateHeartbeatLowWatermarkToViewTopic(
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      long heartbeatTimestamp,
      int version) {
    boolean propagate = false;
    long oldestHeartbeatTimestamp;
    broadcastHBLock.lock();
    try {
      partitionToHeartbeatTimestampMap.put(partition, heartbeatTimestamp);
      long now = clock.millis();
      if (now > lastHBBroadcastTimestamp + DEFAULT_HEARTBEAT_BROADCAST_INTERVAL_MS
          && !partitionToHeartbeatTimestampMap.isEmpty()) {
        oldestHeartbeatTimestamp = Collections.min(partitionToHeartbeatTimestampMap.values());
        if (partition == DEFAULT_PARTITION_TO_ALWAYS_BROADCAST
            || now - oldestHeartbeatTimestamp > DEFAULT_HEARTBEAT_BROADCAST_DELAY_THRESHOLD) {
          propagate = true;
          lastHBBroadcastTimestamp = now;
        }
        partitionToHeartbeatTimestampMap.clear();
      }
    } finally {
      broadcastHBLock.unlock();
    }
    if (propagate) {
      if (veniceWriter == null) {
        initializeVeniceWriter(version);
      }
      LeaderCompleteState leaderCompleteState =
          LeaderCompleteState.getLeaderCompleteState(partitionConsumptionState.isCompletionReported());
      Set<String> failedPartitions = VeniceConcurrentHashMap.newKeySet();
      Set<CompletableFuture<PubSubProduceResult>> heartbeatFutures = VeniceConcurrentHashMap.newKeySet();
      AtomicReference<CompletionException> completionException = new AtomicReference<>();
      for (int p = 0; p < internalView.getViewPartitionCount(); p++) {
        // Due to the intertwined partition mapping, the actual LeaderMetadataWrapper is meaningless for materialized
        // view consumers. Similarly, we will propagate the LeaderCompleteState, but it will only guarantee that at
        // least
        // one partition leader has completed.
        final int viewPartitionNumber = p;
        CompletableFuture<PubSubProduceResult> heartBeatFuture = veniceWriter.sendHeartbeat(
            materializedViewTopicName,
            viewPartitionNumber,
            null,
            VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
            true,
            leaderCompleteState,
            heartbeatTimestamp);
        heartBeatFuture.whenComplete((ignore, throwable) -> {
          if (throwable != null) {
            completionException.set(new CompletionException(throwable));
            failedPartitions.add(String.valueOf(viewPartitionNumber));
          }
        });
        heartbeatFutures.add(heartBeatFuture);
      }
      if (!heartbeatFutures.isEmpty()) {
        CompletableFuture.allOf(heartbeatFutures.toArray(new CompletableFuture[0]))
            .whenCompleteAsync((ignore, throwable) -> {
              if (!failedPartitions.isEmpty()) {
                int failedCount = failedPartitions.size();
                String logMessage = String.format(
                    "Broadcast materialized view heartbeat for %d partitions of topic %s: %d succeeded, %d failed for partitions %s",
                    heartbeatFutures.size(),
                    materializedViewTopicName,
                    heartbeatFutures.size() - failedCount,
                    failedCount,
                    String.join(",", failedPartitions));
                if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
                  LOGGER.error(logMessage, completionException.get());
                }
              }
            });
      }
    }
  }
}
