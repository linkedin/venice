package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


/**
 * This class is used to monitor the various reasons for unhealthy hosts.
 */
public class HostHealthStats extends AbstractVeniceStats {
  private final Sensor unhealthyHostOfflineInstance;
  private final Sensor unhealthyHostSlowPartition;
  private final Sensor unhealthyHostTooManyPendingRequest;
  private final Sensor unhealthyHostHeartBeatFailure;
  private final Sensor pendingRequestCount;

  public HostHealthStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.unhealthyHostOfflineInstance = registerSensor("unhealthy_host_offline_instance", new Count());
    this.unhealthyHostSlowPartition = registerSensor("unhealthy_host_slow_partition", new Count());
    this.unhealthyHostTooManyPendingRequest = registerSensor("unhealthy_host_too_many_pending_request", new Count());
    this.unhealthyHostHeartBeatFailure = registerSensor("unhealthy_host_heart_beat_failure", new Count());
    this.pendingRequestCount = registerSensor("pending_request_count", new Max());
  }

  public void recordUnhealthyHostOfflineInstance() {
    unhealthyHostOfflineInstance.record();
  }

  public void recordUnhealthyHostSlowPartition() {
    unhealthyHostSlowPartition.record();
  }

  public void recordUnhealthyHostTooManyPendingRequest() {
    unhealthyHostTooManyPendingRequest.record();
  }

  public void recordUnhealthyHostHeartBeatFailure() {
    unhealthyHostHeartBeatFailure.record();
  }

  public void recordPendingRequestCount(long cnt) {
    pendingRequestCount.record(cnt);
  }
}
