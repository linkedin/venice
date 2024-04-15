package com.linkedin.davinci.client;

public enum StorageClass {
  /**
   * The mode has the following implications:
   * 1. Keep all the data on disk.
   * 2. Use a block cache to keep the hottest entries and internally it is using LRU algorithm. Internally, Block Cache
   * will try to cache index/bloom filters over data entries if there is a memory pressure.
   * 3. Block cache size is configurable.
   * 4. This mode is recommended for the large store use cases that the application doesn't have enough RAM to keep all
   * the data in memory and the application is using SSD.
   */
  DISK,
  /**
   * The mode has the following implications:
   * 1. Keep a snapshot on disk.
   * 2. Application can use SSD or Hard Drive.
   * 3. Application needs to have enough RAM to keep DaVinci databases fully in RAM (ideally, it should have enough
   * RAM to keep two versions since Venice/DaVinci database is versioned.)
   * 4. At serving time, all the read request will be served out of memory and internally, RocksDB in DaVinci is using
   * mmap to bring the on-disk data files into RAM.
   */
  MEMORY_BACKED_BY_DISK
}
