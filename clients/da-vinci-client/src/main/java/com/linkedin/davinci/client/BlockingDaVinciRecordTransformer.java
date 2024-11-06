package com.linkedin.davinci.client;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.utils.lazy.Lazy;
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

  public BlockingDaVinciRecordTransformer(DaVinciRecordTransformer recordTransformer, boolean storeRecordsInDaVinci) {
    super(recordTransformer.getStoreVersion(), storeRecordsInDaVinci);
    this.recordTransformer = recordTransformer;
  }

  public Schema getKeySchema() {
    return this.recordTransformer.getKeySchema();
  }

  public Schema getOutputValueSchema() {
    return this.recordTransformer.getOutputValueSchema();
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

  public void onStartVersionIngestion() {
    this.recordTransformer.onStartVersionIngestion();
    startLatch.countDown();
  }

  public void onEndVersionIngestion() {
    this.recordTransformer.onEndVersionIngestion();
  }
}
