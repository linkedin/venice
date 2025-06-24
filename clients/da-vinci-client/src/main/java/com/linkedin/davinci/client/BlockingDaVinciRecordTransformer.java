package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.Schema;


/**
 * This is an implementation of {@link DaVinciRecordTransformer} that implements blocking.
 * It ensures that no puts can proceed until onStartIngestionTask finishes.
 *
 * @param <K> type of the input key
 * @param <V> type of the input value
 * @param <O> type of the output value
 */
@Experimental
public class BlockingDaVinciRecordTransformer<K, V, O> extends DaVinciRecordTransformer<K, V, O> {
  private final DaVinciRecordTransformer recordTransformer;

  /**
   * Latch to ensure we complete local RocksDB scan before processing records consumed remotely.
   */
  private final CountDownLatch startLatchConsumptionLatch;

  public BlockingDaVinciRecordTransformer(
      DaVinciRecordTransformer recordTransformer,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(recordTransformer.getStoreVersion(), keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.recordTransformer = recordTransformer;
    this.startLatchConsumptionLatch = new CountDownLatch(recordTransformerConfig.getStartConsumptionLatchCount());
  }

  @Override
  public DaVinciRecordTransformerResult<O> transform(Lazy<K> key, Lazy<V> value, int partitionId) {
    return this.recordTransformer.transform(key, value, partitionId);
  }

  @Override
  public void processPut(Lazy<K> key, Lazy<O> value, int partitionId) {
    this.recordTransformer.processPut(key, value, partitionId);
  }

  /**
   * This is a wrapper method for {@link DaVinciRecordTransformer#transformAndProcessPut(Lazy, Lazy, int)}.
   * It's needed because this method is invoked inside the drainer in the
   * {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask}, and we need to ensure that we finish local RocksDB
   * scan for every partition before we start processing records that have been consumed remotely.
   * Otherwise, local RocksDB scan and remote consumption will compete for resources.
   * We achieve this by awaiting on {@link #startLatchConsumptionLatch}.
   * We also cannot override {@link DaVinciRecordTransformer#transformAndProcessPut(Lazy, Lazy, int)}, since it's a
   * final method and user implementations of {@link DaVinciRecordTransformer} should never override it.
   */
  public DaVinciRecordTransformerResult<O> internalTransformAndProcessPut(Lazy<K> key, Lazy<V> value, int partitionId) {
    try {
      startLatchConsumptionLatch.await();
      return this.recordTransformer.transformAndProcessPut(key, value, partitionId);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This is a wrapper method for {@link DaVinciRecordTransformer#processDelete(Lazy, int)}. it's invoked inside the
   * drainer in the {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask}. In this method we await on
   * {@link #startLatchConsumptionLatch} to ensure we complete local RocksDB scan for every partition before processing
   * records consumed remotely. This method also won't get invoked when performing local RocksDB scan, as we only
   * receive delete events from remote consumption.
   */
  @Override
  public void processDelete(Lazy<K> key, int partitionId) {
    try {
      startLatchConsumptionLatch.await();
      this.recordTransformer.processDelete(key, partitionId);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    this.recordTransformer.onStartVersionIngestion(isCurrentVersion);
  }

  @Override
  public void onEndVersionIngestion(int currentVersion) {
    this.recordTransformer.onEndVersionIngestion(currentVersion);
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
   * on the contents of BlockingDaVinciRecordTransformer, not the user's implementation of DVRT.
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

  @VisibleForTesting
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
