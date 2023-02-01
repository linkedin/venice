package com.linkedin.venice.pubsub.api;

import java.util.Iterator;


public class PubSubMessages<K, V> implements Iterable<PubSubMessage<K, V, Long>> {
  @Override
  public Iterator<PubSubMessage<K, V, Long>> iterator() {
    return null;
  }
}
