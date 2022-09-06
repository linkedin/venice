package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.MemoryUsageType;


/**
 * Class that exposes RocksDB memory consumption stats based on all properties
 * that are made available in https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L870
 *
 * Properties exist on a per RockDB database basis (equivalent to a Venice partition).
 * This class aggregates across the partitions and emits aggregate stats
 */
public class RocksDBMemoryStats extends AbstractVeniceStats {
  private static final Logger logger = LogManager.getLogger(RocksDBMemoryStats.class);

  // List of metric domains to emit
  static final List<String> PARTITION_METRIC_DOMAINS = Arrays.asList(
      "rocksdb.num-immutable-mem-table",
      "rocksdb.mem-table-flush-pending",
      "rocksdb.compaction-pending",
      "rocksdb.background-errors",
      "rocksdb.cur-size-active-mem-table",
      "rocksdb.cur-size-all-mem-tables",
      "rocksdb.size-all-mem-tables",
      "rocksdb.num-entries-active-mem-table",
      "rocksdb.num-entries-imm-mem-tables",
      "rocksdb.num-deletes-active-mem-table",
      "rocksdb.num-deletes-imm-mem-tables",
      "rocksdb.estimate-num-keys",
      "rocksdb.estimate-table-readers-mem",
      "rocksdb.num-snapshots",
      "rocksdb.num-live-versions",
      "rocksdb.estimate-live-data-size",
      "rocksdb.min-log-number-to-keep",
      "rocksdb.total-sst-files-size",
      "rocksdb.live-sst-files-size",
      "rocksdb.estimate-pending-compaction-bytes",
      "rocksdb.num-running-compactions",
      "rocksdb.num-running-flushes",
      "rocksdb.actual-delayed-write-rate",
      "rocksdb.block-cache-capacity",
      "rocksdb.block-cache-pinned-usage",
      "rocksdb.block-cache-usage");

  // metrics emitted on a per instance basis need only be collected once, not aggregated
  private static final Set<String> INSTANCE_METRIC_DOMAINS = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
              "rocksdb.block-cache-capacity",
              "rocksdb.block-cache-pinned-usage",
              "rocksdb.block-cache-usage")));

  // metrics related to block cache, which should not be collected when plain table format is enabled.
  private static final Set<String> BLOCK_CACHE_METRICS =
      PARTITION_METRIC_DOMAINS.stream().filter(s -> s.contains("rocksdb.block-cache")).collect(Collectors.toSet());

  private static final String ROCKSDB_MEMORY_USAGE_SUFFIX = ".rocksdb.memory-usage";

  private static final Set<MemoryUsageType> MEMORY_USAGE_TYPES = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
              MemoryUsageType.kMemTableTotal,
              MemoryUsageType.kTableReadersTotal,
              MemoryUsageType.kCacheTotal)));

  private Map<String, RocksDBStoragePartition> hostedRocksDBPartitions = new ConcurrentHashMap<>();

  private Map<String, MemoryInfo> storeMemoryInfos = new ConcurrentHashMap<>();

  public RocksDBMemoryStats(MetricsRepository metricsRepository, String name, boolean plainTableEnabled) {
    super(metricsRepository, name);
    for (String metric: PARTITION_METRIC_DOMAINS) {
      // Skip the block cache related metrics when using PlainTable format is enabled.
      if (plainTableEnabled && BLOCK_CACHE_METRICS.contains(metric)) {
        continue;
      }
      registerSensor(metric, new Gauge(() -> {
        Long total = 0L;
        // Lock down the list of RocksDB interfaces while the collection is ongoing
        synchronized (hostedRocksDBPartitions) {
          for (RocksDBStoragePartition dbPartition: hostedRocksDBPartitions.values()) {
            try {
              total += dbPartition.getRocksDBStatValue(metric);
            } catch (VeniceException e) {
              logger.warn(String.format("Could not get rocksDB metric %s with error:", metric), e);
              continue;
            }
            if (INSTANCE_METRIC_DOMAINS.contains(metric)) {
              // Collect this metric once from any available partition and move on
              break;
            }
          }
        }
        return total;
      }));
    }
  }

  public void registerPartition(String partitionName, RocksDBStoragePartition rocksDBPartition) {
    hostedRocksDBPartitions.put(partitionName, rocksDBPartition);
  }

  public void deregisterPartition(String partitionName) {
    // Synchronize on the hosted partitions so that this method does not return while
    // a metric collection is ongoing. This prevents venice-server from potentially
    // closing a RocksDB database while a property is being read.
    synchronized (hostedRocksDBPartitions) {
      hostedRocksDBPartitions.remove(partitionName);
    }
  }

  class MemoryInfo {
    private static final double RATIO = 0.1;
    private final String storeName;
    private final long limit;
    private volatile long size;
    private AtomicLong bufferSize;

    MemoryInfo(String storeName, long limit) {
      this.storeName = storeName;
      this.limit = limit;
      this.size = 0;
      this.bufferSize = new AtomicLong(0L);
    }

    private synchronized boolean innerIsFull(long bytesWritten) {
      size = getTotalMemoryUsageInBytes();
      boolean isFull = size + bytesWritten >= limit;
      if (isFull) {
        bufferSize.set(0L);
      } else {
        bufferSize.set(bytesWritten);
      }
      return isFull;
    }

    public boolean isFull(long bytesWritten) {
      // unlimited memory
      if (limit == 0) {
        return false;
      }
      // lazy evaluate size when the first query comes
      // bufferSize limit = 10% of available memory space
      if (size == 0L || bufferSize.addAndGet(bytesWritten) >= RATIO * (limit - size)) {
        return innerIsFull(bytesWritten);
      }
      return false;
    }

    // getTotalMemoryUsageInBytes does not include memory usage in the block cache since RocksDB
    // does not provide api to query memory used by a RocksDB in the block cache
    // when the block cache is shared by multiple RocksDB instances
    private long getTotalMemoryUsageInBytes() {
      long total = 0;

      for (Map.Entry<String, RocksDBStoragePartition> entry: hostedRocksDBPartitions.entrySet()) {
        if (!entry.getKey().startsWith(storeName)) {
          continue;
        }
        Map<MemoryUsageType, Long> memoryUsages = entry.getValue().getApproximateMemoryUsageByType(null);
        for (Map.Entry<MemoryUsageType, Long> memoryUsageEntry: memoryUsages.entrySet()) {
          if (MEMORY_USAGE_TYPES.contains(memoryUsageEntry.getKey())) {
            total += memoryUsageEntry.getValue();
          }
        }
      }

      return total;
    }

    public String getStoreName() {
      return storeName;
    }

    public long getLimit() {
      return limit;
    }

    public synchronized long getTotalSize() {
      return size + bufferSize.get();
    }
  }

  public void registerStore(String storeName, long limit) {
    storeMemoryInfos.computeIfAbsent(storeName, name -> {
      MemoryInfo memoryInfo = new MemoryInfo(name, limit);
      if (limit != 0) {
        String metric = storeName + ROCKSDB_MEMORY_USAGE_SUFFIX;
        registerSensor(metric, new Gauge(memoryInfo::getTotalSize));
      }
      return memoryInfo;
    });
  }

  public boolean isMemoryFull(String storeName, long bytesWritten) {
    MemoryInfo memoryInfo = storeMemoryInfos.get(storeName);
    // store not registered
    if (memoryInfo == null || memoryInfo.getLimit() == 0) {
      return false;
    }
    return memoryInfo.isFull(bytesWritten);
  }
}
