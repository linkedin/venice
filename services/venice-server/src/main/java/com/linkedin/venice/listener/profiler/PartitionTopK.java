package com.linkedin.venice.listener.profiler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


/**
 * Min-heap tracking the top-K hottest key hashes observed for a single partition.
 *
 * <p>The heap is keyed on the hashed key bytes (PII-safe). When {@link #update} is called with a
 * count that beats the current min, the heap evicts its root and inserts the new entry.
 *
 * <p>Synchronization is scoped to this object — one partition's updates do not block another
 * partition's updates.
 */
final class PartitionTopK {
  private final int capacity;
  private final PriorityQueue<Entry> minHeap;
  private final Map<ByteBuffer, Entry> index;

  PartitionTopK(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.capacity = capacity;
    this.minHeap = new PriorityQueue<>(capacity, Comparator.comparingLong(e -> e.count));
    this.index = new HashMap<>(capacity * 2);
  }

  /**
   * Update the heap with the current estimated count for {@code keyHash}.
   *
   * <p>If the key is already tracked, its count is bumped and the heap reheapified. Otherwise, if
   * the heap is below capacity, the key is added; if at capacity, the key replaces the current
   * minimum only when {@code count} strictly exceeds it.
   */
  synchronized void update(byte[] keyHash, long count) {
    ByteBuffer wrappedKey = ByteBuffer.wrap(keyHash);
    Entry existing = index.get(wrappedKey);
    if (existing != null) {
      if (count > existing.count) {
        minHeap.remove(existing);
        existing.count = count;
        minHeap.offer(existing);
      }
      return;
    }
    if (minHeap.size() < capacity) {
      Entry entry = new Entry(keyHash, count);
      minHeap.offer(entry);
      index.put(wrappedKey, entry);
      return;
    }
    Entry weakest = minHeap.peek();
    if (weakest != null && count > weakest.count) {
      minHeap.poll();
      index.remove(ByteBuffer.wrap(weakest.keyHash));
      Entry entry = new Entry(keyHash, count);
      minHeap.offer(entry);
      index.put(wrappedKey, entry);
    }
  }

  /** Snapshot of the heap sorted hottest-first. Not thread-safe to mutate after capture. */
  synchronized List<Entry> snapshotSortedDescending() {
    List<Entry> snapshot = new ArrayList<>(minHeap);
    snapshot.sort((a, b) -> Long.compare(b.count, a.count));
    return snapshot;
  }

  synchronized int size() {
    return minHeap.size();
  }

  int capacity() {
    return capacity;
  }

  static final class Entry {
    final byte[] keyHash;
    long count;

    Entry(byte[] keyHash, long count) {
      this.keyHash = keyHash;
      this.count = count;
    }
  }
}
