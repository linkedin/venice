package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
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
  public DaVinciRecordTransformerResult<O> transform(Lazy<K> key, Lazy<V> value, int partitionId) {
    return this.recordTransformer.transform(key, value, partitionId);
  }

  @Override
  public void processPut(Lazy<K> key, Lazy<O> value, int partitionId) {
    this.recordTransformer.processPut(key, value, partitionId);
  }

  @Override
  public void processDelete(Lazy<K> key, int partitionId) {
    this.recordTransformer.processDelete(key, partitionId);
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    this.recordTransformer.onStartVersionIngestion(isCurrentVersion);
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
   *
   * It is used for DVRT CDC.
   */
  public void onVersionSwap(int currentVersion, int futureVersion, int partitionId) {
    if (this.recordTransformer instanceof BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerBootstrappingChangelogConsumer) {
      ((BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerBootstrappingChangelogConsumer) this.recordTransformer)
          .onVersionSwap(currentVersion, futureVersion, partitionId);
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
      Lazy<VeniceCompressor> compressor) {
    this.recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
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

  @Override
  public void close() throws IOException {
    this.recordTransformer.close();
  }
}
