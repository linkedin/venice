package com.linkedin.venice.pubsub.api;

import java.util.Collections;
import java.util.List;


public class EmptyPubSubMessageHeaders extends PubSubMessageHeaders {
  public static final PubSubMessageHeaders SINGLETON = new EmptyPubSubMessageHeaders();

  private static final String EX_MSG = EmptyPubSubMessageHeaders.class.getSimpleName() + " is immutable.";

  private EmptyPubSubMessageHeaders() {
  }

  @Override
  public PubSubMessageHeaders add(PubSubMessageHeader header) {
    throw new UnsupportedOperationException(EX_MSG);
  }

  @Override
  public PubSubMessageHeaders add(String key, byte[] value) {
    throw new UnsupportedOperationException(EX_MSG);
  }

  @Override
  public PubSubMessageHeaders remove(String key) {
    throw new UnsupportedOperationException(EX_MSG);
  }

  @Override
  public List<PubSubMessageHeader> toList() {
    return Collections.emptyList();
  }

  public boolean isEmpty() {
    return true;
  }

  @Override
  public int getHeapSize() {
    // This is the point of using a singleton!
    return 0;
  }
}
