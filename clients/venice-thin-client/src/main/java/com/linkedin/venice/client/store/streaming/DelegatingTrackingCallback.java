package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.stats.ClientStats;
import java.util.Optional;


public class DelegatingTrackingCallback<K, V> implements TrackingStreamingCallback<K, V> {
  private final TrackingStreamingCallback<K, V> inner;

  public DelegatingTrackingCallback(StreamingCallback<K, V> inner) {
    this.inner = wrap(inner);
  }

  @Override
  public void onRecordReceived(K key, V value) {
    inner.onRecordReceived(key, value);
  }

  @Override
  public void onCompletion(Optional<Exception> exception) {
    inner.onCompletion(exception);
  }

  public Optional<ClientStats> getStats() {
    return inner.getStats();
  }

  public void onRecordDeserialized() {
    inner.onRecordDeserialized();
  }

  public void onDeserializationCompletion(Optional<Exception> exception, int successKeyCount, int duplicateEntryCount) {
    inner.onDeserializationCompletion(exception, successKeyCount, duplicateEntryCount);
  }

  public static <K, V> TrackingStreamingCallback<K, V> wrap(StreamingCallback<K, V> callback) {
    if (callback instanceof TrackingStreamingCallback) {
      return (TrackingStreamingCallback<K, V>) callback;
    }
    return new TrackingStreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        callback.onRecordReceived(key, value);
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        callback.onCompletion(exception);
      }

      @Override
      public Optional<ClientStats> getStats() {
        return Optional.empty();
      }

      @Override
      public void onRecordDeserialized() {
      }

      @Override
      public void onDeserializationCompletion(
          Optional<Exception> exception,
          int successKeyCount,
          int duplicateEntryCount) {
      }
    };
  }
}
