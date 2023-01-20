package com.linkedin.venice.utils;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * A map implementation with a bounded size.
 */
public class BoundedHashMap<K, V> extends LinkedHashMap<K, V> {
  private static final long serialVersionUID = 1L;
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private final int maxSize;

  public BoundedHashMap(int maxSize, boolean accessOrder) {
    super(maxSize, DEFAULT_LOAD_FACTOR, accessOrder);
    this.maxSize = maxSize;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() > maxSize;
  }
}
