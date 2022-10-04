package com.linkedin.alpini.base.misc;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Simple {@link ArrayList} backed {@link Map} implementation which uses simple linear scans of the
 * list for searching for matching keys. Ideal for very small maps where the cost of hash computation
 * may be considered excessive and {@linkplain Object#equals(Object)} of the key is cheap.
 * Permits {@literal null} as a key value.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author acurtis
 * Not thread safe.
 */
@NotThreadSafe
public class ArrayMap<K, V> extends AbstractMap<K, V> {
  protected final ArrayList<Entry<K, V>> _entries;

  private transient Set<Entry<K, V>> _entrySet;
  private final BiPredicate<K, K> _keyEquals;

  private ArrayMap(ArrayList<Entry<K, V>> entries, BiPredicate<K, K> keyEquals) {
    _entries = entries;
    _keyEquals = Objects.requireNonNull(keyEquals);
  }

  public ArrayMap() {
    this(Objects::equals);
  }

  public ArrayMap(BiPredicate<K, K> keyEquals) {
    this(new ArrayList<>(), keyEquals);
  }

  public ArrayMap(int initialSize) {
    this(initialSize, Objects::equals);
  }

  public ArrayMap(int initialSize, BiPredicate<K, K> keyEquals) {
    this(new ArrayList<>(initialSize), keyEquals);
  }

  public ArrayMap(Map<K, V> source) {
    this(source.size(), source instanceof ArrayMap ? ((ArrayMap<K, V>) source)._keyEquals : Objects::equals);
    // no need to check for duplicate keys as we can assume that the source map already does
    // not have any duplicate key entries.
    source.forEach((key, value) -> _entries.add(new SimpleEntry<>(key, value)));
  }

  public final boolean keyEquals(K a, K b) {
    return _keyEquals.test(a, b);
  }

  /** {@inheritDoc} */
  @Override
  public V put(K key, V value) {
    Iterator<Entry<K, V>> i = _entries.iterator();
    while (i.hasNext()) {
      Entry<K, V> e = i.next();
      if (keyEquals(key, e.getKey())) {
        return e.setValue(value);
      }
    }
    _entries.add(new SimpleEntry<>(key, value));
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Set<Entry<K, V>> entrySet() {
    if (_entrySet == null) {
      _entrySet = new AbstractSet<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return _entries.listIterator();
        }

        @Override
        public int size() {
          return _entries.size();
        }

        @Override
        public void clear() {
          _entries.clear();
        }
      };
    }
    return _entrySet;
  }
}
