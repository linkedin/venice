package com.linkedin.venice.fastclient.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.linkedin.venice.meta.CurrentVersionType;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A weight-bounded cache of value bytes exactly as they arrived from the server, keyed by version and serialized key.
 *
 * <p>The cache is enabled only when a positive byte budget is configured on
 * {@link com.linkedin.venice.fastclient.ClientConfig}; otherwise it is never instantiated.
 *
 * <p>Hybrid entries expire 120 seconds after insertion, independently of metadata refreshes; hits do not extend
 * their lifetime. Batch-only entries have no time-based expiration and survive until version swap.
 * The client clears hybrid entries on successful metadata refreshes, and both version types on observed version swaps.
 * These policies follow the current serving version, not store-level configuration changes.
 * Caching is bypassed when metadata does not identify whether the current serving version is hybrid.
 *
 * <p>Values are retained compressed and serialized. Keeping them compressed fits more keys into the same budget for
 * stores that enable compression, at the cost of decompressing on each hit. Keeping them serialized means the memory
 * estimate includes the retained byte lengths, and concurrent callers never share a mutable record instance.
 *
 * <p>Versioned keys isolate late responses from previous versions, even after a version-swap clear. Callers use
 * their request-pinned version for both lookups and fills.
 */
public class FastClientRecordCache {
  private static final Logger LOGGER = LogManager.getLogger(FastClientRecordCache.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  /**
   * Conservative allowance for the map/node, key/value objects, array headers and alignment. This is an estimate,
   * Java 17 instrumentation of Caffeine 2.8.5 measured 184 bytes for a one-byte key/value with compressed pointers,
   * and 264 without them (including both map and eviction nodes). Allow 320 bytes plus payload for headroom.
   * This is not a hard heap cap: shared cache structures and transient request allocations are not weighed.
   */
  static final int ENTRY_OVERHEAD_IN_BYTES = 320;
  private static final long HYBRID_ENTRY_AGE_IN_SECONDS = 120;

  private final Cache<CacheKey, CachedValue> cache;
  private final long configuredBudgetInBytes;
  private final String storeName;
  private final String notInitializedFilterKey;
  private final String unknownTypeFilterKey;
  private final String typeMismatchFilterKey;
  private final Supplier<CurrentVersionType> currentVersionTypeSupplier;
  private volatile CurrentVersionType configuredVersionType = CurrentVersionType.UNKNOWN;
  private volatile boolean closed;

  public FastClientRecordCache(
      String storeName,
      long maxSizeInBytes,
      Supplier<CurrentVersionType> currentVersionTypeSupplier,
      Ticker ticker) {
    if (maxSizeInBytes <= 0) {
      throw new IllegalArgumentException("maxSizeInBytes must be positive, got: " + maxSizeInBytes);
    }
    this.storeName = storeName;
    this.configuredBudgetInBytes = maxSizeInBytes;
    this.notInitializedFilterKey = "Record cache bypass not_initialized for " + storeName;
    this.unknownTypeFilterKey = "Record cache bypass unknown_version_type for " + storeName;
    this.typeMismatchFilterKey = "Record cache bypass version_type_mismatch for " + storeName;
    this.currentVersionTypeSupplier = Objects.requireNonNull(currentVersionTypeSupplier);
    this.cache = Caffeine.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(HYBRID_ENTRY_AGE_IN_SECONDS, TimeUnit.SECONDS)
        .maximumWeight(maxSizeInBytes)
        .weigher((CacheKey key, CachedValue value) -> (int) entryWeight(key.serializedKeyBytes.length, value.size()))
        .build();
  }

  /**
   * @return the cached entry, or null if absent.
   */
  public CachedValue get(byte[] serializedKey, int version) {
    return isConfiguredForCurrentVersion() ? cache.getIfPresent(new CacheKey(version, serializedKey)) : null;
  }

  /** Call before copying batch response bytes; put rechecks eligibility when a retry winner eventually fills. */
  public boolean canAdmit(int keyLength, int valueLength) {
    long weight = entryWeight(keyLength, valueLength);
    try {
      return weight <= configuredBudgetInBytes && weight <= Integer.MAX_VALUE && isConfiguredForCurrentVersion();
    } catch (Exception e) {
      // Failed eligibility checks skip caching, not delivery of the server value.
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException("Record cache admission failed for " + storeName)) {
        LOGGER.warn("Record cache admission failed for store {}; skipping cache write", storeName, e);
      }
      return false;
    }
  }

  private static long entryWeight(int keyLength, int valueLength) {
    return (long) keyLength + valueLength + ENTRY_OVERHEAD_IN_BYTES;
  }

  /** Value bytes must be independently owned, not a slice of a batch response, and must not be mutated after insertion. */
  public void put(byte[] serializedKey, byte[] value, int schemaId, int version) {
    if (!canAdmit(serializedKey.length, value.length)) {
      return;
    }
    CacheKey key = new CacheKey(version, Arrays.copyOf(serializedKey, serializedKey.length));
    cache.put(key, new CachedValue(value, schemaId));
    // Close may have cleared the cache after the eligibility check but before this insertion.
    if (closed) {
      cache.invalidate(key);
    }
  }

  public void invalidate(byte[] serializedKey, int version, CachedValue expectedValue) {
    cache.asMap().remove(new CacheKey(version, serializedKey), expectedValue);
  }

  private boolean isConfiguredForCurrentVersion() {
    if (closed) {
      return false;
    }
    CurrentVersionType configured = configuredVersionType;
    CurrentVersionType current = currentVersionTypeSupplier.get();
    if (current == CurrentVersionType.UNKNOWN) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(unknownTypeFilterKey)) {
        LOGGER.info("Record cache bypass for store {}: unknown_version_type", storeName);
      }
      return false;
    }
    if (configured == CurrentVersionType.UNKNOWN) {
      logNotInitialized();
      return false;
    }
    // Metadata publishes the serving type before invoking refresh callbacks; bypass during that gap.
    if (configured != current) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(typeMismatchFilterKey)) {
        LOGGER.info(
            "Record cache bypass for store {}: version_type_mismatch; configured {}, current {}",
            storeName,
            configured,
            current);
      }
      return false;
    }
    return true;
  }

  /** Also used when the dispatcher rejects a request before metadata initialization completes. */
  public void logNotInitialized() {
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(notInitializedFilterKey)) {
      LOGGER.info("Record cache bypass for store {}: not_initialized", storeName);
    }
  }

  /**
   * Called after each successful metadata refresh, including the initial load. Configures expiration from the
   * serving version: 120 seconds after writing for hybrid entries, effectively no time-based expiry for batch-only.
   * Failed refreshes leave the existing policy and entry ages unchanged.
   *
   * <p>Clears entries on a type change before applying the new lifetime, and on every successful hybrid refresh.
   * Unchanged batch-only refreshes preserve entries; version-swap invalidation is handled separately.
   * Unknown types clear entries and disable caching until a successful refresh supplies a known type.
   */
  public void onMetadataRefresh() {
    if (closed) {
      return;
    }
    CurrentVersionType currentVersionType = currentVersionTypeSupplier.get();
    if (currentVersionType != CurrentVersionType.HYBRID && currentVersionType != CurrentVersionType.BATCH_ONLY) {
      configuredVersionType = CurrentVersionType.UNKNOWN;
      clear();
      return;
    }
    if (currentVersionType != configuredVersionType) {
      configuredVersionType = CurrentVersionType.UNKNOWN;
      clear();
      long duration = currentVersionType == CurrentVersionType.HYBRID
          ? TimeUnit.SECONDS.toNanos(HYBRID_ENTRY_AGE_IN_SECONDS)
          : Long.MAX_VALUE;
      cache.policy().expireAfterWrite().get().setExpiresAfter(duration, TimeUnit.NANOSECONDS);
      configuredVersionType = currentVersionType;
    } else if (currentVersionType == CurrentVersionType.HYBRID) {
      clear();
    }
  }

  /**
   * Drop every entry.
   */
  public void clear() {
    cache.invalidateAll();
  }

  /**
   * Permanently disable caching and release entries. An insertion racing with close removes its entry before returning.
   * Unlike metadata-refresh clearing, closing does not permit late responses to refill the cache.
   */
  public void close() {
    closed = true;
    clear();
  }

  public long getEntryCount() {
    return cache.estimatedSize();
  }

  /**
   * Caffeine's estimated eviction weight: serialized key bytes, compressed value bytes and a flat entry allowance.
   * This is not measured heap usage. Read only when scraping metrics, never on a request's serving path.
   */
  public long getEstimatedWeightInBytes() {
    return cache.policy().eviction().get().weightedSize().getAsLong();
  }

  /** Force pending eviction work to run. Visible for testing. */
  void cleanUp() {
    cache.cleanUp();
  }

  /**
   * A compressed, serialized value with its writer schema id. Records within the same
   * version may use different writer schemas; metadata supplies their definitions, but the schema id identifies
   * which schema encoded these bytes.
   */
  public static final class CachedValue {
    private final byte[] value;
    private final int schemaId;

    CachedValue(byte[] value, int schemaId) {
      this.value = value;
      this.schemaId = schemaId;
    }

    /**
     * @return a new {@link ByteBuffer} over the cached bytes on every call, so that a reader consuming the buffer
     *         does not change any other reader's position. The backing array is shared for decoding without copying;
     *         callers must not modify its contents.
     */
    public ByteBuffer getValue() {
      return ByteBuffer.wrap(value);
    }

    public int getSchemaId() {
      return schemaId;
    }

    int size() {
      return value.length;
    }
  }

  private static final class CacheKey {
    private final int storeVersion;
    private final byte[] serializedKeyBytes;

    // Retained keys own a copy; lookup-only keys borrow the request's bytes for the duration of the lookup.
    private CacheKey(int storeVersion, byte[] serializedKeyBytes) {
      this.storeVersion = storeVersion;
      this.serializedKeyBytes = serializedKeyBytes;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CacheKey)) {
        return false;
      }
      CacheKey that = (CacheKey) other;
      return storeVersion == that.storeVersion && Arrays.equals(serializedKeyBytes, that.serializedKeyBytes);
    }

    @Override
    public int hashCode() {
      return 31 * storeVersion + Arrays.hashCode(serializedKeyBytes);
    }
  }
}
