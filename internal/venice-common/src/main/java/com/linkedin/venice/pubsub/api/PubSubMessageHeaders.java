package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.utils.collections.MeasurableLinkedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * Set of key-value pairs to tagged with messages produced to a topic.
 * In case of headers with the same key, only the most recently added headers value will be kept.
 */
public class PubSubMessageHeaders implements Measurable, Iterable<PubSubMessageHeader> {
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(PubSubMessageHeaders.class);
  /**
   * N.B.: Kafka allows duplicate keys in the headers but some pubsub systems may not
   * allow it. Hence, we will enforce uniqueness of keys in headers from the beginning.
   */
  private final MeasurableLinkedHashMap<String, PubSubMessageHeader> headers = new MeasurableLinkedHashMap<>();

  public static final String VENICE_TRANSPORT_PROTOCOL_HEADER = "vtp";
  /** Header to denote whether the leader is completed or not */
  public static final String VENICE_LEADER_COMPLETION_STATE_HEADER = "lcs";

  public PubSubMessageHeaders add(PubSubMessageHeader header) {
    headers.put(header.key(), header);
    return this;
  }

  public PubSubMessageHeaders add(String key, byte[] value) {
    add(new PubSubMessageHeader(key, value));
    return this;
  }

  public PubSubMessageHeaders remove(String key) {
    headers.remove(key);
    return this;
  }

  /**
   * @return the headers as a List<PubSubMessageHeader>.
   *    Mutating this list will not affect the PubSubMessageHeaders.
   *    If no headers are present an empty list is returned.
   */
  public List<PubSubMessageHeader> toList() {
    return headers.isEmpty() ? Collections.emptyList() : new ArrayList<>(headers.values());
  }

  public boolean isEmpty() {
    return headers.isEmpty();
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD + this.headers.getHeapSize();
  }

  @Override
  public Iterator<PubSubMessageHeader> iterator() {
    return headers.values().iterator();
  }
}
