package com.linkedin.venice.utils;

import java.util.Map;


/**
 * The IndexedMap interface implements Map and several functions for dealing with its
 * content via the index corresponding to their insertion order. The concept is similar
 * to the LinkedHashMap, but affords the user a greater control over how the data is
 * laid out when iterating over it.
 */
public interface IndexedMap<K, V> extends Map<K, V> {
  int indexOf(K key);

  V putByIndex(K key, V value, int index);

  Entry<K, V> getByIndex(int index);

  Entry<K, V> removeByIndex(int index);

  void moveElement(int originalIndex, int newIndex);
}
