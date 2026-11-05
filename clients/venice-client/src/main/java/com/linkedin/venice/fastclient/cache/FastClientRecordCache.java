package com.linkedin.venice.fastclient.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.compression.CompressionStrategy;
import java.nio.ByteBuffer;


/**
 * A memory-bounded cache of value bytes exactly as they arrived from the server, keyed by serialized key bytes.
 *
 * <p>The cache is enabled only when a positive byte budget is configured on
 * {@link com.linkedin.venice.fastclient.ClientConfig}; otherwise it is never instantiated.
 *
 * <p>For batch-only stores, entries are invalidated when a store-metadata refresh detects a version swap;
 * ordinary metadata refreshes retain the cache because values within a batch-only version do not change.
 * For hybrid stores, entries are invalidated on every successful store-metadata refresh because nearline
 * writes can update values within the same version.
 *
 * <p>Values are retained compressed and serialized. Keeping them compressed fits more keys into the same budget for
 * stores that enable compression, at the cost of decompressing on each hit. Keeping them serialized means the memory
 * bound is measured in real bytes, and that concurrent callers never share a mutable record instance.
 *
 * <p>Compressed bytes are only decodable by the compressor of the version they were read from, since
 * {@link CompressionStrategy#ZSTD_WITH_DICT} dictionaries are per version. Each entry therefore records its source
 * version so the caller can reject an entry left behind by a superseded version and resolve its compression
 * strategy from metadata.
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

  public void put(ByteBuffer serializedKey, byte[] value, int schemaId, int version) {
    if (value == null) {
      return;
    }
    cache.put(serializedKey, new CachedValue(value, schemaId, version));
  }

  /**
   * Drop every entry.
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
   * An immutable compressed, serialized value plus its schema id and source store version. The compression
   * strategy is resolved from metadata for that version.
   */
  public static final class CachedValue {
    private final byte[] value;
    private final int schemaId;
    private final int version;

    CachedValue(byte[] value, int schemaId, int version) {
      this.value = value;
      this.schemaId = schemaId;
      this.version = version;
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

    public int getVersion() {
      return version;
    }

    int size() {
      return value.length;
    }
  }
}
