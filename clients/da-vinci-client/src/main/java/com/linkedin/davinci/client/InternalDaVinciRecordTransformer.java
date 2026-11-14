package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.Schema;


/**
 * This is an implementation of {@link DaVinciRecordTransformer} that is used internally inside the
 * {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask}.
 *
 * @param <K> type of the input key
 * @param <V> type of the input value
 * @param <O> type of the output value
 */
@Experimental
public class InternalDaVinciRecordTransformer<K, V, O> extends DaVinciRecordTransformer<K, V, O> {
  private final DaVinciRecordTransformer recordTransformer;
  private final InternalDaVinciRecordTransformerConfig internalConfig;
  private final List<DaVinciRecordTransformer> recoveryTransformers;
  private final Map<DaVinciRecordTransformerConfig, DaVinciRecordTransformerChangelogConsumer> cdcTemplates =
      new ConcurrentHashMap<>();
  private final Map<DaVinciRecordTransformerChangelogConsumer, Map<Integer, DaVinciRecordTransformerChangelogConsumer>> cdcPartitions =
      new ConcurrentHashMap<>();

  /**
   * Latch to ensure we complete local RocksDB scan before resuming remote consumption.
   */
  private final CountDownLatch startLatchConsumptionLatch;

  public InternalDaVinciRecordTransformer(
      DaVinciRecordTransformer recordTransformer,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig) {
    this(recordTransformer, keySchema, inputValueSchema, outputValueSchema, internalRecordTransformerConfig, null);
  }

  private InternalDaVinciRecordTransformer(
      DaVinciRecordTransformer recordTransformer,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig,
      List<DaVinciRecordTransformer> recoveryTransformers) {
    super(
        recordTransformer.getStoreName(),
        recordTransformer.getStoreVersion(),
        keySchema,
        inputValueSchema,
        outputValueSchema,
        internalRecordTransformerConfig.getRecordTransformerConfig());
    this.recordTransformer = recordTransformer;
    this.internalConfig = internalRecordTransformerConfig;
    this.recoveryTransformers = recoveryTransformers;
    this.startLatchConsumptionLatch =
        new CountDownLatch(internalRecordTransformerConfig.getStartConsumptionLatchCount());
  }

  private List<DaVinciRecordTransformer> getPartitionTransformers(int partition) {
    if (recoveryTransformers != null) {
      return recoveryTransformers;
    }
    if (!isCDCRecordTransformer()) {
      return Collections.singletonList(recordTransformer);
    }
    cdcTemplates.keySet().retainAll(internalConfig.getClientConfigs());
    cdcPartitions.keySet().retainAll(cdcTemplates.values());
    List<DaVinciRecordTransformer> transformers = new ArrayList<>();
    for (DaVinciRecordTransformerConfig config: internalConfig.getClientConfigs()) {
      DaVinciRecordTransformerChangelogConsumer template = cdcTemplates.computeIfAbsent(config, clientConfig -> {
        DaVinciRecordTransformer transformer = clientConfig == internalConfig.getRecordTransformerConfig()
            ? recordTransformer
            : clientConfig.getRecordTransformerFunction()
                .apply(
                    getStoreName(),
                    getStoreVersion(),
                    getKeySchema(),
                    getInputValueSchema(),
                    getOutputValueSchema(),
                    clientConfig);
        if (!(transformer instanceof DaVinciRecordTransformerChangelogConsumer)) {
          throw new IllegalStateException("Only CDC clients can share CDC ingestion callbacks");
        }
        return (DaVinciRecordTransformerChangelogConsumer) transformer;
      });
      if (template.isCurrentSubscription(partition)) {
        transformers.add(
            cdcPartitions.computeIfAbsent(template, ignored -> new ConcurrentHashMap<>())
                .compute(partition, (ignored, previous) -> {
                  if (previous != null && previous.isCurrentSubscription(partition)) {
                    return previous;
                  }
                  return template.forPartition(partition);
                }));
      }
    }
    return transformers;
  }

  /**
   * Capture client ownership and subscription generations before recovery is dispatched asynchronously.
   */
  public InternalDaVinciRecordTransformer<K, V, O> forPartitionRecovery(int partition) {
    return isCDCRecordTransformer()
        ? new InternalDaVinciRecordTransformer<>(
            recordTransformer,
            getKeySchema(),
            getInputValueSchema(),
            getOutputValueSchema(),
            internalConfig,
            getPartitionTransformers(partition))
        : this;
  }

  @Override
  public DaVinciRecordTransformerResult<O> transform(
      Lazy<K> key,
      Lazy<V> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return this.recordTransformer.transform(key, value, partitionId, recordMetadata);
  }

  @Override
  public void processPut(
      Lazy<K> key,
      Lazy<O> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
      transformer.processPut(key, value, partitionId, recordMetadata);
    }
  }

  @Override
  public void processDelete(Lazy<K> key, int partitionId, DaVinciRecordTransformerRecordMetadata recordMetadata) {
    for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
      transformer.processDelete(key, partitionId, recordMetadata);
    }
  }

  @Override
  public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
    for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
      transformer.onStartVersionIngestion(partitionId, isCurrentVersion);
    }
  }

  @Override
  public void onEndVersionIngestion(int currentVersion) {
    this.recordTransformer.onEndVersionIngestion(currentVersion);
  }

  @Override
  public boolean useUniformInputValueSchema() {
    return this.recordTransformer.useUniformInputValueSchema();
  }

  /**
   * Lifecycle event triggered when a version swap is detected for partitionId, at {@code position} in the topic being
   * ingested.
   * It is used for DVRT CDC.
   */
  public void onVersionSwap(int currentVersion, int futureVersion, int partitionId, PubSubPosition position) {
    if (isCDCRecordTransformer()) {
      for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
        ((DaVinciRecordTransformerChangelogConsumer) transformer)
            .onVersionSwap(currentVersion, futureVersion, partitionId, position);
      }
    }
  }

  public void onVersionSwap(PubSubTopicPartition source, VersionSwap versionSwap, PubSubPosition position) {
    if (isCDCRecordTransformer()) {
      for (DaVinciRecordTransformer transformer: getPartitionTransformers(source.getPartitionNumber())) {
        ((DaVinciRecordTransformerChangelogConsumer) transformer).onVersionSwap(source, versionSwap, position);
      }
    }
  }

  /**
   * Lifecycle event triggered when a heartbeat is detected for partitionId.
   * It is used for DVRT CDC to record latest heartbeat timestamps per partition.
   */
  public void onHeartbeat(int partitionId, long heartbeatTimestamp) {
    if (isCDCRecordTransformer()) {
      for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
        ((DaVinciRecordTransformerChangelogConsumer) transformer).onHeartbeat(partitionId, heartbeatTimestamp);
      }
    }
  }

  /**
   * On receiving a control message for a given partition and offset, we will process it here.
   * It is used for Version Specific CDC.
   */
  public void onControlMessage(
      int partition,
      PubSubPosition offset,
      ControlMessage controlMessage,
      long pubSubMessageTimestamp) {
    onControlMessage(partition, offset, controlMessage, pubSubMessageTimestamp, 0L);
  }

  /**
   * On receiving a control message with a known producer timestamp (e.g. heartbeats where the
   * upstream RT producer time is preserved end-to-end), propagate both timestamps to the buffer.
   */
  public void onControlMessage(
      int partition,
      PubSubPosition offset,
      ControlMessage controlMessage,
      long pubSubMessageTimestamp,
      long producerTimestamp) {
    if (this.recordTransformer instanceof VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer) {
      for (DaVinciRecordTransformer transformer: getPartitionTransformers(partition)) {
        ((DaVinciRecordTransformerChangelogConsumer) transformer)
            .onControlMessage(partition, offset, controlMessage, pubSubMessageTimestamp, producerTimestamp);
      }
    }
  }

  /**
   * Using a wrapper around onRecovery because when calculating the class hash it grabs the name of the current class
   * that is invoking it. If we directly invoke onRecovery from this class, the class hash will be calculated based
   * on the contents of {@link InternalDaVinciRecordTransformer}, not the user's implementation of DVRT.
   * We also can't override onRecovery like the other methods because this method is final and the implementation
   * should never be overridden.
   */
  public void internalOnRecovery(
      StorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Lazy<VeniceCompressor> compressor,
      PubSubContext pubSubContext,
      Map<Integer, Schema> schemaIdToSchemaMap,
      ReadOnlySchemaRepository schemaRepository) {
    for (DaVinciRecordTransformer transformer: getPartitionTransformers(partitionId)) {
      if (isCDCRecordTransformer()
          && !((DaVinciRecordTransformerChangelogConsumer) transformer).isCurrentSubscription(partitionId)) {
        continue;
      }
      transformer.onRecovery(
          storageEngine,
          partitionId,
          partitionStateSerializer,
          compressor,
          pubSubContext,
          schemaIdToSchemaMap,
          schemaRepository);
    }
  }

  public long getCountDownStartConsumptionLatchCount() {
    return this.startLatchConsumptionLatch.getCount();
  }

  /**
   * This method gets invoked when local RocksDB scan has completed for a partition.
   */
  public void countDownStartConsumptionLatch() {
    this.startLatchConsumptionLatch.countDown();
  }

  public boolean isCDCRecordTransformer() {
    return this.recordTransformer instanceof VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer;
  }

  @Override
  public void close() throws IOException {
    this.recordTransformer.close();
  }
}
