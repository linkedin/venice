package com.linkedin.davinci.kafka.consumer;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.davinci.stats.ingestion.heartbeat.AggregatedHeartbeatLagEntry;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.service.AbstractVeniceService;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * This class contains service to periodically refresh all the signals for throttlers and update all registered throttler
 * based on new signal values.
 */
public class AdaptiveThrottlerSignalService extends AbstractVeniceService {
  public static final double READ_LATENCY_P95_LIMIT = 2.0f;
  public static final long HEARTBEAT_LAG_LIMIT = TimeUnit.MINUTES.toMillis(10);
  private final MetricsRepository metricsRepository;
  private final HeartbeatMonitoringService heartbeatMonitoringService;
  private final List<Long> throttlerList = new ArrayList<>();
  private final ScheduledExecutorService updateService = Executors.newSingleThreadScheduledExecutor();
  private boolean singleGetLatencySignal = false;
  private boolean currentLeaderMaxHeartbeatLagSignal = false;
  private boolean currentFollowerMaxHeartbeatLagSignal = false;
  private boolean nonCurrentLeaderMaxHeartbeatLagSignal = false;
  private boolean nonCurrentFollowerMaxHeartbeatLagSignal = false;

  public AdaptiveThrottlerSignalService(
      MetricsRepository metricsRepository,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    this.metricsRepository = metricsRepository;
    this.heartbeatMonitoringService = heartbeatMonitoringService;
  }

  public void registerThrottler() {
    // TODO: Add the throttler into list.
  }

  public void refreshSignalAndThrottler() {
    // Update all the signals in one shot;
    updateReadLatencySignal();
    updateHeartbeatLatencySignal();
    // Update all the throttler
    // TODO: for each throttler, apply the signal();
  }

  void updateReadLatencySignal() {
    Metric hostSingleGetLatencyP95Metric = metricsRepository.getMetric("total--success_request_latency.95thPercentile");
    if (hostSingleGetLatencyP95Metric != null) {
      double hostSingleGetLatencyP95 =
          metricsRepository.getMetric("total--success_request_latency.95thPercentile").value();
      singleGetLatencySignal = hostSingleGetLatencyP95 > READ_LATENCY_P95_LIMIT;
    }
  }

  void updateHeartbeatLatencySignal() {
    AggregatedHeartbeatLagEntry maxLeaderHeartbeatLag = heartbeatMonitoringService.getMaxLeaderHeartbeatLag();
    currentLeaderMaxHeartbeatLagSignal = maxLeaderHeartbeatLag.getCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
    nonCurrentLeaderMaxHeartbeatLagSignal =
        maxLeaderHeartbeatLag.getNonCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
    AggregatedHeartbeatLagEntry maxFollowerHeartbeatLag = heartbeatMonitoringService.getMaxFollowerHeartbeatLag();
    currentFollowerMaxHeartbeatLagSignal =
        maxFollowerHeartbeatLag.getCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
    nonCurrentFollowerMaxHeartbeatLagSignal =
        maxFollowerHeartbeatLag.getNonCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
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

  @Override
  public boolean startInner() throws Exception {
    updateService.scheduleAtFixedRate(this::refreshSignalAndThrottler, 1, 1, TimeUnit.MINUTES);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    updateService.shutdownNow();
  }
}
