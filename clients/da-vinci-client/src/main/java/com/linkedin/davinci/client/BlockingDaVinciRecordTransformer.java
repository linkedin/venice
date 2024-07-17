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

  public BlockingDaVinciRecordTransformer(DaVinciRecordTransformer recordTransformer) {
    super(recordTransformer.getStoreVersion());
    this.recordTransformer = recordTransformer;
  }

  public Schema getKeyOutputSchema() {
    return this.recordTransformer.getKeyOutputSchema();
  }

  public Schema getValueOutputSchema() {
    return this.recordTransformer.getValueOutputSchema();
  }

  public O put(Lazy<K> key, Lazy<V> value) {
    try {
      // Waiting for onStartIngestionTask to complete before proceeding
      startLatch.await();
      return (O) this.recordTransformer.put(key, value);
    } catch (InterruptedException e) {
      // Restore the interrupt status
      Thread.currentThread().interrupt();
      return null;
    }
  }

  public O delete(Lazy<K> key) {
    return (O) this.recordTransformer.delete(key);
  }

  public void onStartIngestionTask() {
    this.recordTransformer.onStartIngestionTask();
    startLatch.countDown();
  }

  public void onEndIngestionTask() {
    this.recordTransformer.onEndIngestionTask();
  }
}
