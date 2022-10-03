package com.linkedin.alpini.base.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface SerializedMap<K, V> extends Map<K, V> {
  /**
   * Set the block size for the map.
   * @param blockSize block size in bytes
   * @return {@code this} map
   */
  default @Nonnull SerializedMap<K, V> setBlockSize(int blockSize) {
    return this;
  }

  /**
   * Set the maximum age of a block before it may be forgotten.
   * @param time Time in units
   * @param unit Unit of time
   * @return {@code this} map
   */
  default @Nonnull SerializedMap<K, V> setMaxBlockAge(long time, @Nonnull TimeUnit unit) {
    return this;
  }

  /**
   * Set the age at which no further new objects are appended to a new block. A block leaves incubation after
   * this time has elapsed or it is too full to accept further objects.
   * @param time Time in units
   * @param unit Unit of time
   * @return {@code this} map
   */
  default @Nonnull SerializedMap<K, V> setIncubationAge(long time, @Nonnull TimeUnit unit) {
    return this;
  }

  /**
   * Set the maximum amount of bytes to be consumed by the map before blocks are forgotten.
   * @param memory max memory in bytes
   * @return {@code this} map
   */
  default @Nonnull SerializedMap<K, V> setMaxAllocatedMemory(long memory) {
    return this;
  }

  /**
   * Return the block size for the stored entries.
   * @return block size in bytes.
   */
  int getBlockSize();

  /**
   * Return the current maximum age for entries in the map before they are "forgotten".
   * @param unit Unit of time for returned value.
   * @return time in units or {@linkplain Long#MAX_VALUE} for infinite.
   */
  default long getMaxBlockAge(@Nonnull TimeUnit unit) {
    return Long.MAX_VALUE;
  }

  /**
   * Return the age that a block is incubated.
   * @param unit Unit of time for returned value.
   * @return time in units or {@linkplain Long#MAX_VALUE} for infinite.
   */
  default long getIncubationAge(@Nonnull TimeUnit unit) {
    return Long.MAX_VALUE;
  }

  /**
   * Return the maximum number of bytes that this map should be permitted to allocate.
   * @return max bytes
   */
  long getMaxAllocatedMemory();

  /**
   * Returns the number of bytes currently consumed by the serialized map
   * @return number of bytes
   */
  long getAllocatedBytes();

  /**
   * Removes an entry from the map, similarly to {@link Map#remove(Object)}, except returning a {@code boolean}
   * to indicate success without deserialization of the previously stored value.
   * @param key Key of entry to be removed from map
   * @return {@code true} if an entry was successfully removed
   */
  boolean removeEntry(K key);

}
