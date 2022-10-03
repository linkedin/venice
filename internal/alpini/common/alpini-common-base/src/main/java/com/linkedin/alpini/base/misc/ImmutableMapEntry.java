package com.linkedin.alpini.base.misc;

import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A {@link Pair} which may be used as a {@link java.util.Map.Entry}.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public final class ImmutableMapEntry<K, V> extends Pair<K, V> implements Map.Entry<K, V> {
  public ImmutableMapEntry(K key, V value) {
    super(key, value);
  }

  public static <K, V> Map.Entry<K, V> entry(K key, V value) {
    return make(key, value);
  }

  public static <K, V> ImmutableMapEntry<K, V> make(K key, V value) {
    return new ImmutableMapEntry<>(key, value);
  }

  public static <K, V> ImmutableMapEntry<K, V> make(@Nonnull Map.Entry<K, V> entry) {
    return make(entry.getKey(), entry.getValue());
  }

  @Override
  public final K getKey() {
    return getFirst();
  }

  @Override
  public final V getValue() {
    return getSecond();
  }

  @Override
  public final V setValue(V value) {
    throw new UnsupportedOperationException("Not supported");
  }
}
