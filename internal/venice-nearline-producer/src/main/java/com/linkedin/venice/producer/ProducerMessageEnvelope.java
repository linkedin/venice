package com.linkedin.venice.producer;

public class ProducerMessageEnvelope {
  private final Object key;
  private final Object value;

  public ProducerMessageEnvelope(Object key, Object value) {
    this.key = key;
    this.value = value;
  }

  public Object getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }
}
