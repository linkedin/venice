package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.SstFileManager;


/**
 * Class that exposes RocksDB memory consumption stats based on all properties
 * that are made available in https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L870
 *
 * Properties exist on a per RockDB database basis (equivalent to a Venice partition).
 * This class aggregates across the partitions and emits aggregate stats
 */
public class RocksDBMemoryStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBMemoryStats.class);

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
  private volatile long memoryLimit = -1;
  private volatile SstFileManager sstFileManager;

  // metrics related to block cache, which should not be collected when plain table format is enabled.
  private static final Set<String> BLOCK_CACHE_METRICS =
      PARTITION_METRIC_DOMAINS.stream().filter(s -> s.contains("rocksdb.block-cache")).collect(Collectors.toSet());

  private Map<String, RocksDBStoragePartition> hostedRocksDBPartitions = new ConcurrentHashMap<>();

  public RocksDBMemoryStats(MetricsRepository metricsRepository, String name, boolean plainTableEnabled) {
    super(metricsRepository, name);
    for (String metric: PARTITION_METRIC_DOMAINS) {
      // Skip the block cache related metrics when using PlainTable format is enabled.
      if (plainTableEnabled && BLOCK_CACHE_METRICS.contains(metric)) {
        continue;
      }
      registerSensor(new AsyncGauge((c, t) -> {
        Long total = 0L;
        // Lock down the list of RocksDB interfaces while the collection is ongoing
        synchronized (hostedRocksDBPartitions) {
          for (RocksDBStoragePartition dbPartition: hostedRocksDBPartitions.values()) {
            try {
              total += dbPartition.getRocksDBStatValue(metric);
            } catch (VeniceException e) {
              LOGGER.warn("Could not get rocksDB metric {} with error:", metric, e);
              continue;
            }
            if (INSTANCE_METRIC_DOMAINS.contains(metric)) {
              // Collect this metric once from any available partition and move on
              break;
            }
          }
        }
        return total;
      }, metric));
    }
    registerSensor(new AsyncGauge((c, t) -> memoryLimit, "memory_limit"));
    registerSensor(new AsyncGauge((c, t) -> {
      if (memoryLimit > 0 && sstFileManager != null) {
        return sstFileManager.getTotalSize();
      }
      return -1;
    }, "memory_usage"));
  }

  public void setMemoryLimit(long memoryLimit) {
    this.memoryLimit = memoryLimit;
  }

  public void setSstFileManager(SstFileManager sstFileManager) {
    this.sstFileManager = sstFileManager;
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
}
