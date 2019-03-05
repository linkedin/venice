package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Optional;
import java.util.concurrent.Executor;


public abstract class StreamingCallback<K, V> {

  public StreamingCallback() {
  }

  /**
   * This function will be invoked when some records are ready to be consumed.
   *
   * This function could be invoked in parallel when data records arrive at the same time,
   * and if you want to need to do sequential processing, you need to synchronize it in
   * the customized {@link StreamingCallback#onRecordReceived(Object, Object)}.
   *
   * @param key
   * @param value : could be null when key doesn't exist in Venice.
   */
  public abstract void onRecordReceived(K key, V value);

  /**
   * This will be invoked when the callbacks are fully executed.
   *
   * @param exception Exception thrown when processing result from Venice.
   */
  public abstract void onCompletion(Optional<Exception> exception);

}
