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
 * <p>Entries are invalidated wholesale on every successful store-metadata refresh, which bounds staleness by the
 * metadata refresh interval. Because the Fast Client only observes a store version swap during a metadata refresh,
 * clearing on refresh also covers version swaps.
 *
 * <p>Values are retained compressed and serialized. Keeping them compressed fits more keys into the same budget for
 * stores that enable compression, at the cost of decompressing on each hit. Keeping them serialized means the memory
 * bound is measured in real bytes, and that concurrent callers never share a mutable record instance.
 *
 * <p>Compressed bytes are only decodable by the compressor of the version they were read from, since
 * {@link CompressionStrategy#ZSTD_WITH_DICT} dictionaries are per version. Each entry therefore records its source
 * version and strategy so the caller can reject an entry left behind by a superseded version.
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

  public void put(
      ByteBuffer serializedKey,
      byte[] value,
      int schemaId,
      CompressionStrategy compressionStrategy,
      int version) {
    if (value == null) {
      return;
    }
    cache.put(serializedKey, new CachedValue(value, schemaId, compressionStrategy, version));
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
   * An immutable compressed, serialized value plus everything needed to decode it: the schema id, and the
   * compression strategy and store version it was read from.
   */
  public static final class CachedValue {
    private final byte[] value;
    private final int schemaId;
    private final CompressionStrategy compressionStrategy;
    private final int version;

    CachedValue(byte[] value, int schemaId, CompressionStrategy compressionStrategy, int version) {
      this.value = value;
      this.schemaId = schemaId;
      this.compressionStrategy = compressionStrategy;
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

    public CompressionStrategy getCompressionStrategy() {
      return compressionStrategy;
    }

    public int getVersion() {
      return version;
    }

    int size() {
      return value.length;
    }
  }
}
