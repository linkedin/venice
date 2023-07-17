package com.linkedin.venice.client.store.streaming;

public abstract class ClientComputeStreamingCallback<K, V> extends StreamingCallback<K, V> {
  public abstract void onRawRecordReceived(K key, V value);

  public abstract void onRemoteComputeStateChange(boolean enabled);
}
