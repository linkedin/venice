package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.stats.ClientStats;
import java.util.Optional;


/**
 * This interface exposes one more function: {@link #onDeserializationCompletion(Optional, int, int)}, which
 * will be used for metric tracking.
 */
public interface TrackingStreamingCallback<K, V> extends StreamingCallback<K, V> {
  Optional<ClientStats> getStats();

  /**
   * This will be invoked when any record deserialization happens.
   */
  void onRecordDeserialized();

  /**
   * This will be invoked when Venice Client deserialization is done.
   * @param exception
   * @param successKeyCount, this param indicates the total number of existing keys
   * @param duplicateEntryCount
   */
  void onDeserializationCompletion(Optional<Exception> exception, int successKeyCount, int duplicateEntryCount);
}
