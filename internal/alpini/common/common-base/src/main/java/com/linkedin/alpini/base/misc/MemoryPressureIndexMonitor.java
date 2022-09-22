package com.linkedin.alpini.base.misc;

import java.lang.ref.PhantomReference;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Monitor using the size of live objects, byte-based to indicate memory pressure.
 * @param <T> The referent that contains bytes information, for example, an HttpRequest that has Content-Length in
 *           its header
 * @param <K> The Key used by internal map to keep track of byte count by objects.
 * @param <S> The Stats
 * @author solu
 */
public class MemoryPressureIndexMonitor<T, K, S> {
  private static final Logger LOG = LogManager.getLogger(MemoryPressureIndexMonitor.class);
  // The adder records total in and out Bytes.
  // The Overflow risk is small as per Antony:
  // According to my calculations, assuming that we transfer 1GB of data per second (max 10ge NIC) for 24 hours per day,
  // 365 days per year, it will take 272.2044146576142057201497055756490679200843125972807044709 years
  private final LongAdder _totalInBytes = new LongAdder();
  private final LongAdder _totalOutBytes = new LongAdder();

  private final ConcurrentMap<K, ByteCount<T, K, S>> _byteCountConcurrentHashMap = new ConcurrentHashMap<>();

  public Function<T, Optional<K>> getIdSupplier() {
    return _idSupplier;
  }

  // Function that takes a generic T as input and return a K type as the key used by byCountMap
  private final Function<T, Optional<K>> _idSupplier;

  // usually this should be the Stats' value setter to receive the update of the MPI
  private BiConsumer<S, Long> _statsValueConsumer;

  private final S _stats;

  private boolean _phantomMode = true;

  /**
   * Create a Monitor by specifying the a function that takes a T as input and returns a Long as the key used
    by byCountMap.
   *
   * @param idSupplier idSupplier
   */
  public MemoryPressureIndexMonitor(
      Function<T, Optional<K>> idSupplier,
      S stats,
      BiConsumer<S, Long> statsValueConsumer) {
    // Make sure there is no link between T and the Long by constructing a new Long every time.
    // This may have performance penalty but it is safer for GC.
    this._idSupplier = Objects.requireNonNull(idSupplier, "idSupplier");
    this._stats = Objects.requireNonNull(stats, "stats");
    this._statsValueConsumer = Objects.requireNonNull(statsValueConsumer, "statsValueConsumer");
  }

  public MemoryPressureIndexMonitor<T, K, S> setPhantomMode(boolean phantomMode) {
    this._phantomMode = phantomMode;
    return this;
  }

  public static class ByteCount<T, K, S> {
    // Ue AtomicLong instead of LongAdder as this is a counter per Referent
    private final AtomicLong _count = new AtomicLong(0);
    private PhantomReference<T> _phantom;
    // Since WeakReference could lose the grip about its referent, the .get() could return null.
    // So we use a _hashCode to cache the hashCode result.
    private final int _hashCode;
    // use this to cache System.identityHashCode(referent) for equals to use.;
    // Not a 100% bulletproof but good enough.
    private final int _systemIdentityHashCode;
    private final MemoryPressureIndexMonitor<T, K, S> _memoryPressureIndexMonitor;

    public ByteCount(T referent, K key, MemoryPressureIndexMonitor<T, K, S> memoryPressureIndexMonitor) {
      this._hashCode = referent.hashCode();
      this._systemIdentityHashCode = System.identityHashCode(referent);
      this._memoryPressureIndexMonitor =
          Objects.requireNonNull(memoryPressureIndexMonitor, "memoryPressureIndexMonitor");
      Objects.requireNonNull(key, "key");
      if (_memoryPressureIndexMonitor._phantomMode) {
        this._phantom = LeakDetect.newReference(referent, () -> {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Finally removed {} bytes from request key={}", count(), key);
          }
          _memoryPressureIndexMonitor._totalOutBytes.add(count());
          _memoryPressureIndexMonitor._byteCountConcurrentHashMap.remove(key);
          _memoryPressureIndexMonitor.updateStats();
          _phantom.clear();
        });
      }
    }

    public ByteCount<T, K, S> add(long delta) {
      if (delta < 0) {
        throw new IllegalArgumentException(String.format("Illegal input %d", delta));
      }
      _count.addAndGet(delta);
      _memoryPressureIndexMonitor._totalInBytes.add(delta);
      return this;
    }

    public long count() {
      return _count.get();
    }

    @Override
    public int hashCode() {
      // Warning: we can't use _id.get().hashCode here as _id is a WeakReference and its referent could be
      // garbage collected therefore, the _id.get() could return null.
      return this._hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      // Using raw type but we don't care here.
      if (!(o instanceof ByteCount)) {
        return false;
      }
      ByteCount other = (ByteCount) o;

      // Warning: we can't use _id.get() here as _id is a WeakReference and its referent could be garbage collected.
      // therefore, the _id.get() could return null.
      // So first of all, we try our best to obtain the referent of this and other. If they are not null, we use them
      // for equals. Otherwise, we shall fall back to using _hashCode and _systemIdentityHashcode
      return this._hashCode == other._hashCode && this._systemIdentityHashCode == other._systemIdentityHashCode;
    }
  }

  public long getBytesByReferent(T referent) {
    if (referent == null) {
      return 0;
    }
    Optional<K> key = _idSupplier.apply(referent);
    return key.isPresent() && _byteCountConcurrentHashMap.containsKey(key.get())
        ? _byteCountConcurrentHashMap.get(key.get()).count()
        : 0;
  }

  public boolean isPhantomSetForReferent(T referent) {
    if (referent == null) {
      return false;
    }
    Optional<K> key = _idSupplier.apply(referent);
    return key.isPresent() && _byteCountConcurrentHashMap.containsKey(key.get())
        ? _byteCountConcurrentHashMap.get(key.get())._phantom != null
        : false;
  }

  /**
   * Remove the capacity and return the volume that would be removed in this method or deferred.
   * @param key key
   * @param immediatelyReduceCountAndRemoveEntry immediatelyReduceCountAndRemoveEntry
   * @param valueToBeAdded valueToBeAdded
   * @return the Optional of ByteCount
   */
  public Optional<ByteCount<T, K, S>> removeByteCount(
      K key,
      boolean immediatelyReduceCountAndRemoveEntry,
      Optional<Integer> valueToBeAdded) {
    Objects.requireNonNull(key, "key");
    return Optional.ofNullable(_byteCountConcurrentHashMap.computeIfPresent(key, (k, v) -> {
      // if immediatelyReduceCountAndRemoveEntry is true, the entry will be removed (by returning null) and the
      // _totalOutBytes is immediately changed. Otherwise all the actions are deferred to LeakDetect
      if (immediatelyReduceCountAndRemoveEntry) {
        _totalOutBytes.add(v.count());
        updateStats();
        return null;
      } else {
        // if there is a valueToBeAdded, add it into ByteCount
        valueToBeAdded.ifPresent(v::add);
      }
      return v;
    }));
  }

  /**
   * Update ByteCount and add a PhantomReference to defer the ByteCount removal to it.
   * @param key key
   * @param valueToBeAdded valueToBeAdded
   * @return ref
   */
  public Optional<ByteCount<T, K, S>> removeByteCountAndAddPhantom(K key, Optional<Integer> valueToBeAdded) {
    Objects.requireNonNull(key, "key");
    return removeByteCount(key, false, valueToBeAdded);
  }

  /**
   * Add an referent if it is not in the map, other wise add the ByteCount.
   * @param referent reference
   * @param bytes bytes in flight
   * @return current memory pressure index
   */
  public long addReferentAndByteCount(T referent, long bytes) {
    Objects.requireNonNull(referent, "The reference must not be null.");
    _idSupplier.apply(referent)
        .map(key -> _byteCountConcurrentHashMap.computeIfAbsent(key, k -> new ByteCount<>(referent, key, this)))
        .ifPresent(byteCount -> byteCount.add(bytes));
    updateStats();
    return currentMemoryPressureIndex();
  }

  public long currentMemoryPressureIndex() {
    return _totalInBytes.longValue() - _totalOutBytes.longValue();
  }

  /**
   * For test purpose. clearing the map would lose all the ByteCounts then the PhantomReference won't work
   */
  public void reset() {
    _totalOutBytes.reset();
    _totalInBytes.reset();
    _byteCountConcurrentHashMap.clear();
  }

  private void updateStats() {
    _statsValueConsumer.accept(_stats, currentMemoryPressureIndex());
  }
}
