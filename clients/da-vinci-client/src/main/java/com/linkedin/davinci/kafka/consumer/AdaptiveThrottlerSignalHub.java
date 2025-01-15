package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.ingestion.heartbeat.AggregatedHeartbeatLagEntry;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import io.tehuti.metrics.MetricsRepository;


/**
 * This class contains host-level signals for adaptive ingestion throttling logic.
 */
public class AdaptiveThrottlerSignalHub {
  private final MetricsRepository metricsRepository;
  private final HeartbeatMonitoringService heartbeatMonitoringService;

  private boolean singleGetLatencySignal = false;
  private boolean currentLeaderMaxHeartbeatLagSignal = false;
  private boolean currentFollowerMaxHeartbeatLagSignal = false;
  private boolean nonCurrentLeaderMaxHeartbeatLagSignal = false;
  private boolean nonCurrentFollowerMaxHeartbeatLagSignal = false;

  public AdaptiveThrottlerSignalHub(
      MetricsRepository metricsRepository,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    this.metricsRepository = metricsRepository;
    this.heartbeatMonitoringService = heartbeatMonitoringService;
  }

  public void refreshSignal() {
    updateReadLatencySignal();
    updateHeartbeatLatencySignal();
  }

  void updateReadLatencySignal() {
    double hostSingleGetLatencyP95 =
        metricsRepository.getMetric("total--success_request_latency.95thPercentile").value();
    singleGetLatencySignal = hostSingleGetLatencyP95 > 2.0d;
  }

  void updateHeartbeatLatencySignal() {
    AggregatedHeartbeatLagEntry maxLeaderHeartbeatLag = heartbeatMonitoringService.getMaxLeaderHeartbeatLag();
    currentLeaderMaxHeartbeatLagSignal = maxLeaderHeartbeatLag.getCurrentVersionHeartbeatLag() > 600;
    nonCurrentLeaderMaxHeartbeatLagSignal = maxLeaderHeartbeatLag.getNonCurrentVersionHeartbeatLag() > 600;
    AggregatedHeartbeatLagEntry maxFollowerHeartbeatLag = heartbeatMonitoringService.getMaxFollowerHeartbeatLag();
    currentFollowerMaxHeartbeatLagSignal = maxFollowerHeartbeatLag.getCurrentVersionHeartbeatLag() > 600;
    nonCurrentFollowerMaxHeartbeatLagSignal = maxFollowerHeartbeatLag.getNonCurrentVersionHeartbeatLag() > 600;
  }

  public boolean isSingleGetLatencySignalActive() {
    return singleGetLatencySignal;
  }

  public boolean isCurrentLeaderMaxHeartbeatLagSignalActive() {
    return currentLeaderMaxHeartbeatLagSignal;
  }

  public boolean isCurrentFollowerMaxHeartbeatLagSignalActive() {
    return currentFollowerMaxHeartbeatLagSignal;
  }

  public boolean isNonCurrentLeaderMaxHeartbeatLagSignalActive() {
    return nonCurrentLeaderMaxHeartbeatLagSignal;
  }

  public boolean isNonCurrentFollowerMaxHeartbeatLagSignalActive() {
    return nonCurrentFollowerMaxHeartbeatLagSignal;
  }
}
