package com.linkedin.venice.listener.profiler;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.concurrent.atomic.AtomicLongArray;


/**
 * Minimal Count-Min Sketch for approximate frequency estimation.
 *
 * <p>The sketch is a {@code depth x width} table of {@code long} counters. Each {@code add} writes
 * to one counter per row using an independent murmur3 hash. {@code estimateCount} returns the min
 * across the row buckets — over-counts are possible (from hash collisions) but under-counts are
 * not. With the defaults below, error is bounded by {@code 0.1% * N} where {@code N} is the total
 * number of observed events. Using {@code long} counters lets a single session safely cover hours
 * of high-QPS traffic without overflow (an {@code int} would cap at ~2.1B per bucket).
 *
 */
final class CountMinSketch {
  static final int DEFAULT_WIDTH = 2718;
  static final int DEFAULT_DEPTH = 5;

  private final int width;
  private final int depth;
  private final AtomicLongArray[] rows;
  private final HashFunction[] hashFunctions;

  CountMinSketch() {
    this(DEFAULT_WIDTH, DEFAULT_DEPTH);
  }

  CountMinSketch(int width, int depth) {
    if (width <= 0 || depth <= 0) {
      throw new IllegalArgumentException("width and depth must be positive");
    }
    this.width = width;
    this.depth = depth;
    this.rows = new AtomicLongArray[depth];
    this.hashFunctions = new HashFunction[depth];
    for (int row = 0; row < depth; row++) {
      this.rows[row] = new AtomicLongArray(width);
      // Distinct seeds yield independent hash functions for the row buckets.
      this.hashFunctions[row] = Hashing.murmur3_32_fixed(0x9E3779B1 + row);
    }
  }

  void add(byte[] key, long count) {
    for (int row = 0; row < depth; row++) {
      int bucket = bucketOf(key, row);
      rows[row].getAndAdd(bucket, count);
    }
  }

  long estimateCount(byte[] key) {
    long min = Long.MAX_VALUE;
    for (int row = 0; row < depth; row++) {
      int bucket = bucketOf(key, row);
      long count = rows[row].get(bucket);
      if (count < min) {
        min = count;
      }
    }
    return min;
  }

  /**
   * Hot-path fast variant: add {@code count} and return the post-add CMS estimate in a single
   * pass through the rows. Saves the second iteration ({@code depth} murmur3 hashes + {@code
   * depth} bucket lookups) compared to calling {@link #add} followed by {@link #estimateCount}.
   */
  long addAndEstimate(byte[] key, long count) {
    long min = Long.MAX_VALUE;
    for (int row = 0; row < depth; row++) {
      int bucket = bucketOf(key, row);
      long updated = rows[row].addAndGet(bucket, count);
      if (updated < min) {
        min = updated;
      }
    }
    return min;
  }

  int width() {
    return width;
  }

  int depth() {
    return depth;
  }

  long memoryBytes() {
    return (long) depth * width * Long.BYTES;
  }

  private int bucketOf(byte[] key, int row) {
    int hash = hashFunctions[row].hashBytes(key).asInt();
    return Math.floorMod(hash, width);
  }
}
