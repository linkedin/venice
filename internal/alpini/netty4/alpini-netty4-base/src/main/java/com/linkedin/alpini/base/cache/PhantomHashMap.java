package com.linkedin.alpini.base.cache;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A simple wrapper around {@link PhantomHashMap} which makes it look like a standard {@link java.util.Map} collection.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class PhantomHashMap<K, V> extends AbstractMap<K, V> implements Map<K, V> {
  private static final Logger LOG = LogManager.getLogger(PhantomHashMap.class);

  private final PhantomHashCache<K, V> _phantomCache;
  private final Map<K, V> _backingStore;
  private transient Set<Entry<K, V>> _entrySet;
  private transient Set<K> _keySet;

  public PhantomHashMap(PhantomHashCache<K, V> phantomCache, Map<K, V> backingStore) {
    _phantomCache = Objects.requireNonNull(phantomCache);
    _backingStore = Objects.requireNonNull(backingStore);
  }

  @SuppressWarnings("unchecked")
  protected K castKey(Object key) {
    return (K) key;
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Note: Calls the {@linkplain SerializedMap#size()} method
   * </p>
   */
  @Override
  public int size() {
    return _backingStore.size();
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Note: Calls the {@linkplain SerializedMap#isEmpty()} method
   * </p>
   */
  @Override
  public boolean isEmpty() {
    return _backingStore.isEmpty();
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Note: Calls the {@linkplain SerializedMap#containsKey(Object)} method
   * </p>
   */
  @Override
  public boolean containsKey(Object key) {
    return _backingStore.containsKey(key);
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Note: Attempts to retrieve the object by the named key via the phantom cache.
   * </p>
   */
  @Override
  public V get(Object key) {
    try {
      return _phantomCache.get(castKey(key), _backingStore);
    } catch (InterruptedException e) {
      LOG.warn("interrupted", e);
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V put(K key, V value) {
    V oldValue = get(key);
    if (value != null) {
      if (oldValue == null || !value.equals(oldValue)) {
        _phantomCache.put(key, _backingStore, value);
      }
    } else {
      _phantomCache.removeEntry(key, _backingStore);
    }
    return oldValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V remove(Object key) {
    V oldValue = get(key);
    _phantomCache.removeEntry(castKey(key), _backingStore);
    return oldValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    _phantomCache.clear();
    _backingStore.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Set<Entry<K, V>> entrySet() {
    if (_entrySet == null) {
      _entrySet = new AbstractSet<Entry<K, V>>() {
        @Override
        @Nonnull
        public Iterator<Entry<K, V>> iterator() {
          Iterator<K> it = keySet().iterator();
          return new Iterator<Entry<K, V>>() {
            private transient RemovableEntry<K, V> _prevEntry;
            private transient RemovableEntry<K, V> _nextEntry;

            @Override
            public boolean hasNext() {
              if (_nextEntry == null) {
                while (it.hasNext()) {
                  K key = it.next();
                  try {
                    _nextEntry = new RemovableEntry<K, V>() {
                      V _value = _phantomCache.get(key, _backingStore);

                      @Override
                      public K getKey() {
                        return key;
                      }

                      @Override
                      public V getValue() {
                        return _value;
                      }

                      @Override
                      public V setValue(V value) {
                        V oldValue = _value;
                        if (value == null) {
                          remove();
                          _value = null;
                        } else {
                          if (_value == null) {
                            throw new IllegalStateException();
                          }
                          _phantomCache.put(key, _backingStore, value);
                          _value = value;
                        }
                        return oldValue;
                      }

                      @Override
                      public boolean remove() {
                        if (_value == null) {
                          throw new IllegalStateException();
                        }
                        _value = null;
                        return _phantomCache.removeEntry(key, _backingStore);
                      }
                    };
                    if (_nextEntry.getValue() != null) {
                      break;
                    }
                  } catch (InterruptedException e) {
                    LOG.warn("interrupted", e);
                  }
                  _nextEntry = null;
                }
              }
              return _nextEntry != null;
            }

            @Override
            public Entry<K, V> next() {
              if (hasNext()) {
                _prevEntry = _nextEntry;
                _nextEntry = null;
                return _prevEntry;
              }
              throw new IndexOutOfBoundsException();
            }

            @Override
            public void remove() {
              if (_prevEntry != null) {
                _prevEntry.remove();
                _prevEntry = null;
              } else {
                throw new IllegalStateException();
              }
            }
          };
        }

        @Override
        public int size() {
          return PhantomHashMap.this.size();
        }

        @Override
        public boolean remove(Object o) {
          return o instanceof Entry && _phantomCache.removeEntry(castKey(((Entry) o).getKey()), _backingStore);
        }
      };
    }
    return _entrySet;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public Set<K> keySet() {
    if (_keySet == null) {
      _keySet = new AbstractSet<K>() {
        @Override
        @Nonnull
        public Iterator<K> iterator() {
          Iterator<K> it = _backingStore.keySet().iterator();
          return new Iterator<K>() {
            private transient K _prev;

            @Override
            public boolean hasNext() {
              return it.hasNext();
            }

            @Override
            public K next() {
              _prev = it.next();
              return _prev;
            }

            @Override
            public void remove() {
              it.remove();
              _phantomCache.removeEntry(_prev, null);
            }
          };
        }

        @Override
        public int size() {
          return PhantomHashMap.this.size();
        }

        @Override
        public boolean remove(Object o) {
          return _phantomCache.removeEntry(castKey(o), _backingStore);
        }
      };
    }
    return _keySet;
  }

  private interface RemovableEntry<K, V> extends Entry<K, V> {
    boolean remove();
  }
}
