package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.annotation.Experimental;
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
  private final CountDownLatch startLatch = new CountDownLatch(1);

  public BlockingDaVinciRecordTransformer(
      DaVinciRecordTransformer recordTransformer,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(recordTransformer.getStoreVersion(), keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.recordTransformer = recordTransformer;
  }

  public DaVinciRecordTransformerResult<O> transform(Lazy<K> key, Lazy<V> value) {
    return this.recordTransformer.transform(key, value);
  }

  public void processPut(Lazy<K> key, Lazy<O> value) {
    try {
      // Waiting for onStartIngestionTask to complete before proceeding
      startLatch.await();
      this.recordTransformer.processPut(key, value);
    } catch (InterruptedException e) {
      // Restore the interrupt status
      Thread.currentThread().interrupt();
    }
  }

  public void processDelete(Lazy<K> key) {
    this.recordTransformer.processDelete(key);
  }

  public void onStartVersionIngestion(boolean isCurrentVersion) {
    this.recordTransformer.onStartVersionIngestion(isCurrentVersion);
    startLatch.countDown();
  }

  public void onEndVersionIngestion(int currentVersion) {
    this.recordTransformer.onEndVersionIngestion(currentVersion);
  }

  public void internalOnRecovery(
      AbstractStorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Lazy<VeniceCompressor> compressor) {
    // Using a wrapper around onRecovery because when calculating the class hash it grabs the name of the current class
    // that is invoking it. If we directly invoke onRecovery from this class, the class hash will be calculated based
    // on the contents of BlockingDaVinciRecordTransformer, not the user's implementation of DVRT.
    // We also can't override onRecovery like the other methods because this method is final and the implementation
    // should never be overriden.
    this.recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
  }

  @Override
  public void close() throws IOException {
    this.recordTransformer.close();
  }
}
