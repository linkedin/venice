package com.linkedin.alpini.base.cache;

import io.netty.util.ReferenceCountUtil;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A cache backed which uses PhantomReferences to cache instances of values to avoid multiple instances of
 * the same cached object. This cache should not prevent a Value object from being garbage collected.
 *
 * The purpose of this map is to ensure that there is little penalty in storing objects for a short period of time.
 *
 * Objects stored within the map are assumed to be immutable.
 *
 * The {@code clone} function provided to the constructor should return a lightweight clone of the supplied object
 * which references the same objects within it.
 *
 * As long as the keys are unique, it is possible to use multiple backing stores with only one
 * {@linkplain PhantomHashCache} providing a cache.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public final class PhantomHashCache<K, V> {
  private static final Logger LOG = LogManager.getLogger(PhantomHashCache.class);
  private static final ReferenceQueue<Object> REFERENCE_QUEUE = new ReferenceQueue<>();
  public static final int DEFAULT_INITIAL_CAPACITY = 512 * 1024;

  /** Lightweight value clone function */
  private final Function<V, V> _clone;

  /** Map to phantoms */
  final ConcurrentMap<K, Entry> _phantomCache;

  private final Lock _purgeLock = new ReentrantLock(false);
  private long _nextPurgeTime;
  long _purgedEntriesCount;

  /**
   * Creates a new, empty map with the default initial table size (512k).
   *
   * @param clone lightweight clone function
   */
  public PhantomHashCache(@Nonnull Function<V, V> clone) {
    this(clone, DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Creates a new, empty map with an initial table size
   * accommodating the specified number of elements without the need
   * to dynamically resize.
   *
   * @param clone lightweight clone function
   * @param initialCapacity the initial capacity. The implementation
   * performs internal sizing to accommodate this many elements,
   * given the specified load factor.
   * @throws IllegalArgumentException if the initial capacity of
   * elements is negative
   */
  public PhantomHashCache(@Nonnull Function<V, V> clone, int initialCapacity) {
    this(clone, () -> new ConcurrentHashMap<>(initialCapacity));
  }

  /**
   * Creates a new, empty phantom map with an initial table size based on
   * the given number of elements ({@code initialCapacity}) and
   * initial table density ({@code loadFactor}).
   *
   * @param clone lightweight clone function
   * @param initialCapacity the initial capacity. The implementation
   * performs internal sizing to accommodate this many elements,
   * given the specified load factor.
   * @param loadFactor the load factor (table density) for
   * establishing the initial table size
   * @throws IllegalArgumentException if the initial capacity of
   * elements is negative or the load factor is nonpositive
   */
  public PhantomHashCache(@Nonnull Function<V, V> clone, int initialCapacity, float loadFactor) {
    this(clone, initialCapacity, loadFactor, 1);
  }

  /**
   * Creates a new, empty phantom map with an initial table size based on
   * the given number of elements ({@code initialCapacity}), table
   * density ({@code loadFactor}), and number of concurrently
   * updating threads ({@code concurrencyLevel}).
   *
   * @param clone lightweight clone function
   * @param initialCapacity the initial capacity. The implementation
   * performs internal sizing to accommodate this many elements,
   * given the specified load factor.
   * @param loadFactor the load factor (table density) for
   * establishing the initial table size
   * @param concurrencyLevel the estimated number of concurrently
   * updating threads. The implementation may use this value as
   * a sizing hint.
   * @throws IllegalArgumentException if the initial capacity is
   * negative or the load factor or concurrencyLevel are
   * nonpositive
   */
  public PhantomHashCache(@Nonnull Function<V, V> clone, int initialCapacity, float loadFactor, int concurrencyLevel) {
    this(clone, () -> new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel));
  }

  private PhantomHashCache(@Nonnull Function<V, V> clone, @Nonnull Supplier<ConcurrentMap<K, Entry>> mapConstructor) {
    _clone = Objects.requireNonNull(clone);
    _phantomCache = mapConstructor.get();
  }

  /**
   * Returns the number of key-value mappings in this map.
   * Performance of this function is dependent upon the implementation used by
   * {@link ConcurrentHashMap#size()}.
   *
   * @return the number of key-value mappings in this map
   */
  public int size() {
    return _phantomCache.size();
  }

  /**
   * Remove all entries from the map.
   */
  public void clear() {
    _phantomCache.clear();
  }

  public void purge(@Nonnull Map<K, V> store) {
    if (_purgeLock.tryLock()) {
      long purgedCount;
      try {
        long now = System.currentTimeMillis();
        if (now < _nextPurgeTime) {
          return;
        }
        _nextPurgeTime = now + 5000L;
        purgedCount = _phantomCache.values()
            .stream()
            .filter(Entry::hasNoPhantoms)
            .filter(entry -> !store.containsKey(entry.key()))
            .map(Entry::key)
            .collect(Collectors.toList())
            .stream()
            .map(_phantomCache::remove)
            .filter(Objects::nonNull)
            .count();
        if (purgedCount == 0) {
          return;
        }
        _purgedEntriesCount += purgedCount;
      } finally {
        _purgeLock.unlock();
      }
      LOG.debug("Purged: {}, Total Purged: {}", purgedCount, _purgedEntriesCount);
    }
  }

  /**
   * Associates the specified value with the specified key in this map
   * If the map previously contained a mapping for the key, the new value is ignored.
   *
   * If the map did not previously contain a mapping for the key, the new value is stored within the backing store
   * and a {@linkplain PhantomReference} is made to it.
   *
   * @param key key with which the specified value is to be associated
   * @param store backing store which the supplied key/value should be persisted within.
   * @param value value to be associated with the specified key
   */
  public void put(@Nonnull K key, @Nonnull Map<K, V> store, @Nonnull V value) {
    Objects.requireNonNull(store);
    Objects.requireNonNull(value);

    Entry ent = new Entry(Objects.requireNonNull(key));
    Entry current = _phantomCache.putIfAbsent(key, ent);
    if (current == null) {
      store.put(key, ent.setValue(value, store));
    } else {
      current.setValue(value, store);
    }
  }

  /**
   * Removes the mapping for a key from this map if it is present.
   *
   * @param key key whose mapping is to be removed from the map
   * @param store backing store from which the mapping should be removed
   * @return {@code true} if this map contained a mapping for the key
   */
  public boolean removeEntry(@Nonnull K key, Map<K, V> store) {
    try {
      Entry ent = _phantomCache.get(Objects.requireNonNull(key));
      boolean removed = false;
      if (ent != null && ent.purge()) {
        return true;
      } else if (ent != null) {
        removed = _phantomCache.remove(key, ent);
      }
      return (store != null && store.keySet().remove(key)) || removed;
    } finally {
      derefLoop();
    }
  }

  /**
   * Retrieve a lightweight clone of the object from the map if it has not yet been garbage collected.
   *
   * @param key the key whose associated value is to be returned
   * @param store backing store which should be queried if the cache does not have a mapping for the key
   * @return a lightweight clone of the value to which the specified key is mapped, or
   *         {@code null} if this map contains no mapping for the key
   */
  public V get(@Nonnull K key, @Nonnull Map<K, V> store) throws InterruptedException {
    Objects.requireNonNull(store);

    // We construct a new Entry because if there is a miss in the map of Phantoms,
    // this new Entry becomes the placeholder for retrieving the entry from the store.
    // If the store does not have an entry by the key, we will remove the entry.
    // The getCurrent method is synchronized so that we do not end up deserializing the
    // same object from the store on multiple threads.

    V value = new Entry(Objects.requireNonNull(key)).getCurrent(store);
    if (value != null) {
      return value;
    }

    // Cache and store miss. May as well perform necessary cleanup.
    derefLoop();
    return null;
  }

  private interface PhantomEntry {
    void deref(boolean purge);
  }

  private @Nonnull V cloneValue(@Nonnull V value) {
    V clone = _clone.apply(value);
    assert clone != value;
    return clone;
  }

  private final class Entry {
    private final K _key;
    private boolean _deleted;
    private final LinkedList<Phantom> _phantoms = new LinkedList<>();

    private Entry(@Nonnull K key) {
      _key = key;
    }

    private K key() {
      return _key;
    }

    private synchronized boolean hasNoPhantoms() {
      return _phantoms.isEmpty();
    }

    private synchronized boolean deref(Phantom phantom) {
      boolean result = _phantoms.remove(phantom);
      if (result && hasNoPhantoms()) {
        if ((_deleted || !phantom._store.containsKey(key())) && _phantomCache.remove(_key, this)) {
          phantom._store.keySet().remove(_key);
        }
      }
      return result;
    }

    private synchronized V setValue(V value, @Nonnull Map<K, V> store) {
      _phantoms.clear();
      if (value != null) {
        _deleted = false;
        V backing = value;
        value = cloneValue(backing);
        _phantoms.add(new Phantom(value, backing, store, REFERENCE_QUEUE));
      }
      return value;
    }

    private synchronized V fetch(@Nonnull Map<K, V> store) throws InterruptedException {
      // we should not remove phantom just because we are reading from the store.
      Phantom current = _phantoms.peekLast();
      if (current != null) {
        if (_deleted) {
          return current._value;
        }
        V value = cloneValue(current._value);
        _phantoms.add(new Phantom(value, current._value, current._store, REFERENCE_QUEUE));
        _phantoms.removeLastOccurrence(current);
        return value;
      }

      if (!_deleted) {
        V backing = store.get(key());
        if (backing != null) {
          V value = cloneValue(backing);
          _phantoms.add(new Phantom(value, backing, store, REFERENCE_QUEUE));
          return value;
        }
      }

      // If we get here, the object has already been removed from the backing store.
      _phantomCache.remove(key(), this);
      return null;
    }

    private synchronized boolean purge() {
      if (!_phantoms.isEmpty()) {
        _deleted = true;
        return true;
      }
      return false;
    }

    public synchronized V getCurrent(Map<K, V> store) throws InterruptedException {
      final Entry current = _phantomCache.putIfAbsent(_key, this);
      if (current != null) {
        V value = current.fetch(store); // Will retrieve it from backing store if required.
        if (value != null) {
          return value;
        }
        _phantomCache.remove(_key, current);
      } else {
        V value = store.get(_key);
        if (value != null) {
          return setValue(value, store);
        }
        _phantomCache.remove(_key, this);

        // record that this was a cache miss
        _deleted = true;
      }
      return null;
    }

    private final class Phantom extends PhantomReference<Object> implements PhantomEntry {
      private final Map<K, V> _store;
      private final V _value;

      public Phantom(@Nonnull V referent, V value, @Nonnull Map<K, V> store, @Nonnull ReferenceQueue<Object> q) {
        super(referent, q);
        _store = store;
        _value = value;
      }

      public void deref(boolean purge) {
        if (Entry.this.deref(this)) {
          ReferenceCountUtil.release(_value);
        }
        if (purge) {
          PhantomHashCache.this.purge(_store);
        }
      }
    }
  }

  private static void derefLoop() {
    while (deref(REFERENCE_QUEUE.poll(), false)) {
      // Empty
    }
  }

  private static boolean deref(Reference<?> reference, boolean purge) {
    if (reference instanceof PhantomEntry) {
      ((PhantomEntry) reference).deref(purge);
      return true;
    }
    return false;
  }

  private static final Thread INVALIDATOR = new Thread("PhantomHashMapInvalidator") {
    @Override
    public void run() {
      // noinspection InfiniteLoopStatement
      while (true) {
        try {
          deref(REFERENCE_QUEUE.remove(), true);
        } catch (Throwable e) {
          LOG.warn("Interrupted", e);
        }
      }
    }
  };

  static {
    INVALIDATOR.setDaemon(true);
    INVALIDATOR.setPriority(Thread.MAX_PRIORITY);
    INVALIDATOR.start();
  }
}
