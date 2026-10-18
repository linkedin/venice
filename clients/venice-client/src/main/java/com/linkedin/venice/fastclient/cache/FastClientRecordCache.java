package com.linkedin.venice.fastclient.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.ByteBuffer;


/**
 * A memory-bounded cache of decompressed, still-serialized value bytes, keyed by serialized key bytes.
 *
 * <p>The cache is enabled only when a positive byte budget is configured on
 * {@link com.linkedin.venice.fastclient.ClientConfig}; otherwise it is never instantiated.
 *
 * <p>Entries are invalidated wholesale on every successful store-metadata refresh, which bounds staleness by the
 * metadata refresh interval. Because the Fast Client only observes a store version swap during a metadata refresh,
 * clearing on refresh also covers version swaps.
 *
 * <p>Values are retained in serialized form rather than as deserialized records so that the memory bound is
 * measured in real bytes, and so that concurrent callers never share a mutable record instance.
 */
public class FastClientRecordCache {
  /**
   * Flat per-entry allowance for the map node, the key and value object headers, and the byte[] headers. This is an
   * approximation: it keeps the configured budget in the right ballpark without reflective sizing on the read path.
   */
  private static final int ENTRY_OVERHEAD_IN_BYTES = 64;

  private final Cache<ByteBuffer, CachedValue> cache;

  public FastClientRecordCache(long maxSizeInBytes) {
    if (maxSizeInBytes <= 0) {
      throw new IllegalArgumentException("maxSizeInBytes must be positive, got: " + maxSizeInBytes);
    }
    this.cache = Caffeine.newBuilder()
        .maximumWeight(maxSizeInBytes)
        .weigher(
            (
                ByteBuffer key,
                CachedValue value) -> (int) Math
                    .min(Integer.MAX_VALUE, (long) key.remaining() + value.size() + ENTRY_OVERHEAD_IN_BYTES))
        .build();
  }

  /**
   * @return the cached entry, or null if absent.
   */
  public CachedValue get(ByteBuffer serializedKey) {
    return cache.getIfPresent(serializedKey);
  }

  public void put(ByteBuffer serializedKey, byte[] value, int schemaId) {
    if (value == null) {
      return;
    }
    cache.put(serializedKey, new CachedValue(value, schemaId));
  }

  /**
   * Drop every entry. Invoked after each successful metadata refresh.
   */
  public void clear() {
    cache.invalidateAll();
  }

  public long getEntryCount() {
    return cache.estimatedSize();
  }

  /** Force pending eviction work to run. Visible for testing. */
  void cleanUp() {
    cache.cleanUp();
  }

  /**
   * An immutable decompressed value plus the schema id needed to deserialize it.
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
     *         cannot affect any other reader.
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
}
