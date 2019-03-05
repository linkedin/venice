package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Optional;


/**
 * This class exposes one more function: {@link #onDeserializationCompletion(Optional, int, int)}, which
 * will be used for metric tracking.
 */
public abstract class TrackingStreamingCallback<K, V> extends StreamingCallback<K, V> {
  private final StreamingCallback<K, V> inner;

  public TrackingStreamingCallback(StreamingCallback<K, V> inner) {
    this.inner = inner;
  }

  /**
   * This will invoked when any record deserialization happens.
   */
  public abstract void onRecordDeserialized();

  /**
   * This will be invoked when Venice Client deserialization is done.
   * @param veniceException
   * @param resultCnt
   * @param duplicateEntryCnt
   */
  public abstract void onDeserializationCompletion(Optional<VeniceClientException> veniceException,
      int resultCnt, int duplicateEntryCnt);

  @Override
  public void onRecordReceived(K key, V value) {
    inner.onRecordReceived(key, value);
  }

  @Override
  public void onCompletion(Optional<Exception> exception) {
    inner.onCompletion(exception);
  }
}
