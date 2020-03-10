package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


/**
 * Class that exposes RocksDB memory consumption stats based on all properties
 * that are made available in https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L870
 *
 * Properties exist on a per RockDB database basis (equivalent to a Venice partition).
 * This class aggregates across the partitions and emits aggregate stats
 */
public class RocksDBMemoryStats extends AbstractVeniceStats{

  private static final Logger logger = Logger.getLogger(RocksDBMemoryStats.class);

  // List of metric domains to emit
  static final List<String> PARTITON_METRIC_DOMAINS = Arrays.asList(
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
      "rocksdb.min-obsolete-sst-number-to-keep",
      "rocksdb.total-sst-files-size",
      "rocksdb.live-sst-files-size",
      "rocksdb.estimate-pending-compaction-bytes",
      "rocksdb.num-running-compactions",
      "rocksdb.num-running-flushes",
      "rocksdb.actual-delayed-write-rate",
      "rocksdb.block-cache-capacity",
      "rocksdb.block-cache-pinned-usage",
      "rocksdb.block-cache-usage"
      );

  // metrics emitted on a per instance basis need only be collected once, not aggregated
  static final List<String> INSTANCE_METRIC_DOMAINS = Arrays.asList(
      "rocksdb.block-cache-capacity",
      "rocksdb.block-cache-pinned-usage",
      "rocksdb.block-cache-usage"
  );


  private Map<String, RocksDB> hostedRocksDBPartitions = new ConcurrentHashMap<>();

  public RocksDBMemoryStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    for(String metric : PARTITON_METRIC_DOMAINS) {
      registerSensor(metric, new Gauge(() -> {
        Long total = 0L;
        for(RocksDB dbPartition : hostedRocksDBPartitions.values()) {
          try {
            total += Long.parseLong(dbPartition.getProperty(metric));
          } catch (RocksDBException e) {
            logger.warn(String.format("Could not get rocksDB metric %s with error:", metric), e);
            continue;
          }
          if(INSTANCE_METRIC_DOMAINS.contains(metric)) {
            // Collect this metric once from any available partition and move on
            break;
          }
        }
        return total;
      }));
    }
  }

  public void registerPartition(String partitionName, RocksDB rocksDBPartition) {
    hostedRocksDBPartitions.put(partitionName, rocksDBPartition);
  }

  public void deregisterPartition(String partitionName) {
    hostedRocksDBPartitions.remove(partitionName);
  }
}
