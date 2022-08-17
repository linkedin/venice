package com.linkedin.davinci.store.rocksdb;

public enum RocksDBBlockCacheImplementations {
  /**
   * ClockCache implements the CLOCK algorithm. Each shard of the clock cache maintains a circular list of cache entries.
   * A clock handle runs over the circular list looking for unpinned entries to evict, but also giving each entry a
   * second chance to stay in cache if it has been used since last scan.  The benefit over LRUCache is it has
   * finer-granularity locking. In case of LRU cache, the per-shard mutex has to be locked even on lookup,
   * since it needs to update its LRU-list. Looking up from a clock cache won't require locking per-shard mutex,
   * but only looking up the concurrent hash map, which has fine-granularity locking. Only inserts needs to lock the
   * per-shard mutex.
   */
  CLOCK,
  /**
   * Each shard of the cache maintains its own LRU list and its own hash table for lookup.
   * Synchronization is done via a per-shard mutex. Both lookup and insert to the cache would require
   * a locking mutex of the shard.
   */
  LRU
}
