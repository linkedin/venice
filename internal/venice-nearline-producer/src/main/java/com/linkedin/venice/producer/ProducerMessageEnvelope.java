package com.linkedin.venice.producer;

public class ProducerMessageEnvelope {
  private final String storeName;
  private final Object key;
  private final Object value;

  public ProducerMessageEnvelope(String storeName, Object key, Object value) {
    this.storeName = storeName;
    this.key = key;
    this.value = value;
  }

  public String getStoreName() {
    return storeName;
  }

  public Object getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }
}
