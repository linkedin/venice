package com.linkedin.davinci.kafka.consumer;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.ingestion.heartbeat.AggregatedHeartbeatLagEntry;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.service.AbstractVeniceService;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains service to periodically refresh all the signals for throttlers and update all registered throttler
 * based on new signal values.
 */
public class AdaptiveThrottlerSignalService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(AdaptiveThrottlerSignalService.class);
  public static final long HEARTBEAT_LAG_LIMIT = TimeUnit.MINUTES.toMillis(10);
  private static final String SINGLE_GET_LATENCY_P99_METRIC_NAME = "total--success_request_latency.99thPercentile";
  private final double singleGetLatencyP99Threshold;
  private final MetricsRepository metricsRepository;
  private final HeartbeatMonitoringService heartbeatMonitoringService;
  private final List<VeniceAdaptiveIngestionThrottler> throttlerList = new ArrayList<>();
  private final ScheduledExecutorService updateService = Executors.newSingleThreadScheduledExecutor();
  private boolean singleGetLatencySignal = false;
  private boolean currentLeaderMaxHeartbeatLagSignal = false;
  private boolean currentFollowerMaxHeartbeatLagSignal = false;
  private boolean nonCurrentLeaderMaxHeartbeatLagSignal = false;
  private boolean nonCurrentFollowerMaxHeartbeatLagSignal = false;

  public AdaptiveThrottlerSignalService(
      VeniceServerConfig veniceServerConfig,
      MetricsRepository metricsRepository,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    this.singleGetLatencyP99Threshold = veniceServerConfig.getAdaptiveThrottlerSingleGetLatencyThreshold();
    this.metricsRepository = metricsRepository;
    this.heartbeatMonitoringService = heartbeatMonitoringService;
  }

  public void registerThrottler(VeniceAdaptiveIngestionThrottler adaptiveIngestionThrottler) {
    throttlerList.add(adaptiveIngestionThrottler);
  }

  public void refreshSignalAndThrottler() {
    // Update all the signals in one shot;
    updateReadLatencySignal();
    updateHeartbeatLatencySignal();
    // Update all the throttler
    throttlerList.forEach(VeniceAdaptiveIngestionThrottler::checkSignalAndAdjustThrottler);
  }

  void updateReadLatencySignal() {
    Metric hostSingleGetLatencyP99Metric = metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME);
    if (hostSingleGetLatencyP99Metric != null) {
      double hostSingleGetLatencyP99 = metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME).value();
      singleGetLatencySignal = hostSingleGetLatencyP99 > singleGetLatencyP99Threshold;
    }
    LOGGER.info("Update read latency signal. singleGetLatency: {}", singleGetLatencySignal);
  }

  void updateHeartbeatLatencySignal() {
    AggregatedHeartbeatLagEntry maxLeaderHeartbeatLag = heartbeatMonitoringService.getMaxLeaderHeartbeatLag();
    if (maxLeaderHeartbeatLag != null) {
      currentLeaderMaxHeartbeatLagSignal = maxLeaderHeartbeatLag.getCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
      nonCurrentLeaderMaxHeartbeatLagSignal =
          maxLeaderHeartbeatLag.getNonCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
    }
    AggregatedHeartbeatLagEntry maxFollowerHeartbeatLag = heartbeatMonitoringService.getMaxFollowerHeartbeatLag();
    if (maxFollowerHeartbeatLag != null) {
      currentFollowerMaxHeartbeatLagSignal =
          maxFollowerHeartbeatLag.getCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
      nonCurrentFollowerMaxHeartbeatLagSignal =
          maxFollowerHeartbeatLag.getNonCurrentVersionHeartbeatLag() > HEARTBEAT_LAG_LIMIT;
    }
    LOGGER.info(
        "Update heartbeat signal. currentLeader: {}, currentFollower: {}, nonCurrentLeader: {}, nonCurrentFollower: {}",
        currentLeaderMaxHeartbeatLagSignal,
        currentFollowerMaxHeartbeatLagSignal,
        nonCurrentLeaderMaxHeartbeatLagSignal,
        nonCurrentFollowerMaxHeartbeatLagSignal);

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

  List<VeniceAdaptiveIngestionThrottler> getThrottlerList() {
    return throttlerList;
  }
}
