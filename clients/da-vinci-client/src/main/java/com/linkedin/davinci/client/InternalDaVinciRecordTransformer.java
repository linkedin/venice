package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.Map;
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
    super(
        recordTransformer.getStoreName(),
        recordTransformer.getStoreVersion(),
        keySchema,
        inputValueSchema,
        outputValueSchema,
        internalRecordTransformerConfig.getRecordTransformerConfig());
    this.recordTransformer = recordTransformer;
    this.startLatchConsumptionLatch =
        new CountDownLatch(internalRecordTransformerConfig.getStartConsumptionLatchCount());
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
    this.recordTransformer.processPut(key, value, partitionId, recordMetadata);
  }

  @Override
  public void processDelete(Lazy<K> key, int partitionId, DaVinciRecordTransformerRecordMetadata recordMetadata) {
    this.recordTransformer.processDelete(key, partitionId, recordMetadata);
  }

  @Override
  public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
    this.recordTransformer.onStartVersionIngestion(partitionId, isCurrentVersion);
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
   * Lifecycle event triggered when a version swap is detected for partitionId
   * It is used for DVRT CDC.
   */
  public void onVersionSwap(int currentVersion, int futureVersion, int partitionId) {
    if (isCDCRecordTransformer()) {
      ((VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer) this.recordTransformer)
          .onVersionSwap(currentVersion, futureVersion, partitionId);
    }
  }

  /**
   * Lifecycle event triggered when a heartbeat is detected for partitionId.
   * It is used for DVRT CDC to record latest heartbeat timestamps per partition.
   */
  public void onHeartbeat(int partitionId, long heartbeatTimestamp) {
    if (isCDCRecordTransformer()) {
      ((VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer) this.recordTransformer)
          .onHeartbeat(partitionId, heartbeatTimestamp);
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
    this.recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        pubSubContext,
        schemaIdToSchemaMap,
        schemaRepository);
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
