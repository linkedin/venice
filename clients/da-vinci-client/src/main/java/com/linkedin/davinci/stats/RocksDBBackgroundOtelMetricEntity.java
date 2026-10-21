package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;


/** Native environment gauges and database background-job observations, not executor thread counts. */
public enum RocksDBBackgroundOtelMetricEntity implements ModuleMetricEntityInterface, TehutiMetricNameEnum {
  FLUSH_POOL_CAPACITY(
      "rocksdb.flush.pool.capacity", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Threads configured in the shared native HIGH priority environment pool"
  ),
  COMPACTION_POOL_CAPACITY(
      "rocksdb.compaction.pool.capacity", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Threads configured in the shared native LOW priority environment pool"
  ),
  FLUSH_POOL_QUEUE_DEPTH(
      "rocksdb.flush.pool.queue_depth", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Tasks queued in the shared native HIGH priority environment pool"
  ),
  COMPACTION_POOL_QUEUE_DEPTH(
      "rocksdb.compaction.pool.queue_depth", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Tasks queued in the shared native LOW priority environment pool"
  ),
  FLUSH_RUNNING_JOBS(
      "rocksdb.flush.running_jobs", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Collection-time samples of running flush jobs across hosted partitions; not threads or time-weighted utilization"
  ),
  COMPACTION_RUNNING_JOBS(
      "rocksdb.compaction.running_jobs", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
      "Collection-time samples of running compaction jobs across hosted partitions; not threads or time-weighted utilization"
  ),
  FLUSH_COMPLETED_COUNT(
      "rocksdb.flush.completed_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Successful SST flush completion notifications"
  ),
  COMPACTION_COMPLETED_COUNT(
      "rocksdb.compaction.completed_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Compaction completion notifications, including unsuccessful jobs"
  ),
  FLUSH_TIME(
      "rocksdb.flush.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Wall time between matched flush begin and completion callbacks, including commit wait"
  ),
  COMPACTION_TIME(
      "rocksdb.compaction.time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
      "Native compaction elapsed wall time, excluding scheduling queue wait"
  ),
  FLUSH_RAW_BYTES(
      "rocksdb.flush.completed_raw_bytes", MetricType.COUNTER, MetricUnit.BYTES,
      "Raw key and value bytes in completed flush tables; rate gives logical flush throughput, not disk bandwidth"
  ),
  COMPACTION_INPUT_BYTES(
      "rocksdb.compaction.completed_input_bytes", MetricType.COUNTER, MetricUnit.BYTES,
      "Input bytes reported by completed compaction jobs; rate gives compaction input throughput"
  ),
  COMPACTION_OUTPUT_BYTES(
      "rocksdb.compaction.completed_output_bytes", MetricType.COUNTER, MetricUnit.BYTES,
      "Output bytes reported by completed compaction jobs; rate gives compaction output throughput"
  );

  private final MetricEntity metricEntity;

  RocksDBBackgroundOtelMetricEntity(String name, MetricType type, MetricUnit unit, String description) {
    metricEntity = new MetricEntity(name, type, unit, description, setOf(VENICE_CLUSTER_NAME));
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }

  @Override
  public String getMetricName() {
    return metricEntity.getMetricName();
  }
}
