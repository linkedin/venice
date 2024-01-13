package com.linkedin.venice.pubsub.adapter.kafka;

import static com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker.INVALID;

import org.apache.kafka.common.Metric;


/**
 * A wrapper for a Kafka {@link Metric} object representing a topic-partition's lag. We also carry the
 * current offset so that we can calculate the end offset.
 */
class KafkaPartitionTopicOffsetMetrics {
  private long currentOffset = INVALID;
  private Metric lagMetric = null;

  void setCurrentOffset(long currentOffset) {
    this.currentOffset = currentOffset;
  }

  void setLagMetric(Metric lagMetric) {
    this.lagMetric = lagMetric;
  }

  boolean isLagMetricMissing() {
    return this.lagMetric == null;
  }

  long getLag() {
    if (this.lagMetric != null) {
      Object metricValue = this.lagMetric.metricValue();
      if (metricValue instanceof Double) {
        // Double is the way all metrics are internally represented in Kafka, but since we are dealing with lag, we
        // want an integral type, so we cast it.
        return ((Double) metricValue).longValue();
      }
    }
    return INVALID;
  }

  long getEndOffset() {
    if (this.currentOffset == INVALID) {
      return INVALID;
    }
    long lag = getLag();
    if (lag == INVALID) {
      return INVALID;
    }
    return this.currentOffset + lag;
  }
}
