package com.linkedin.davinci.kafka.consumer;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AdaptiveThrottlingServiceStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.AggregatedHeartbeatLagEntry;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.throttle.VeniceAdaptiveThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider.LatencyType;
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
 *
 * <p><b>Read-latency signal source</b> is controlled by
 * {@link com.linkedin.venice.stats.VeniceMetricsConfig#USE_SELF_CONTAINED_STATS}:
 * <ul>
 *   <li>{@code false} — reads p99 from the Tehuti windowed percentile registered by
 *       {@code ServerHttpRequestStats} via {@code metricsRepository.getMetric(...).value()}.</li>
 *   <li>{@code true} — reads p99 from an independent {@link LatencyPercentileProvider} owned by
 *       this service, fed by {@code ServerHttpRequestStats} at recording time, so the signal is
 *       functional even when the Tehuti dependency is removed.</li>
 * </ul>
 */
public class AdaptiveThrottlerSignalService extends AbstractVeniceService {
  public static final long HEARTBEAT_LAG_LIMIT = TimeUnit.MINUTES.toMillis(10);
  public static final String SINGLE_GET_LATENCY_P99_METRIC_NAME = ".total--success_request_latency.99thPercentile";
  public static final String MULTI_GET_LATENCY_P99_METRIC_NAME =
      ".total--multiget_storage_engine_query_latency.99thPercentile";
  public static final String READ_COMPUTE_LATENCY_P99_METRIC_NAME =
      ".total--compute_storage_engine_query_latency.99thPercentile";

  private static final Logger LOGGER = LogManager.getLogger(AdaptiveThrottlerSignalService.class);
  private final double singleGetLatencyP99Threshold;
  private final double multiGetLatencyP99Threshold;
  private final double readComputeLatencyP99Threshold;
  private final int adaptiveThrottlerSignalRefreshIntervalInSeconds;

  private final MetricsRepository metricsRepository;
  private final HeartbeatMonitoringService heartbeatMonitoringService;
  private final List<VeniceAdaptiveThrottler> throttlerList = new ArrayList<>();
  private final ScheduledExecutorService updateService;
  /**
   * Independent read-latency p99 source; non-null only when
   * {@link #useSelfContainedStats} is {@code true}. Exposed via
   * {@link #getLatencyPercentileProvider()} so the recording side (for example
   * {@code ServerHttpRequestStats}) can feed it.
   */
  private final LatencyPercentileProvider latencyPercentileProvider;
  private final boolean useSelfContainedStats;
  private boolean singleGetLatencySignal = false;
  private boolean multiGetLatencySignal = false;
  private boolean readComputeLatencySignal = false;

  private boolean currentLeaderMaxHeartbeatLagSignal = false;
  private boolean currentFollowerMaxHeartbeatLagSignal = false;
  private boolean nonCurrentLeaderMaxHeartbeatLagSignal = false;
  private boolean nonCurrentFollowerMaxHeartbeatLagSignal = false;
  private final AdaptiveThrottlingServiceStats adaptiveThrottlingServiceStats;

  public AdaptiveThrottlerSignalService(
      VeniceServerConfig veniceServerConfig,
      MetricsRepository metricsRepository,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    this.singleGetLatencyP99Threshold = veniceServerConfig.getAdaptiveThrottlerSingleGetLatencyThreshold();
    this.multiGetLatencyP99Threshold = veniceServerConfig.getAdaptiveThrottlerMultiGetLatencyThreshold();
    this.readComputeLatencyP99Threshold = veniceServerConfig.getAdaptiveThrottlerReadComputeLatencyThreshold();
    this.adaptiveThrottlerSignalRefreshIntervalInSeconds =
        veniceServerConfig.getAdaptiveThrottlerSignalRefreshIntervalInSeconds();
    this.metricsRepository = metricsRepository;
    this.useSelfContainedStats = VeniceMetricsConfig.useSelfContainedStats(metricsRepository);
    this.latencyPercentileProvider = useSelfContainedStats ? new LatencyPercentileProvider() : null;
    this.updateService = Executors.newSingleThreadScheduledExecutor(
        new DaemonThreadFactory("AdaptiveThrottlerSignalService", veniceServerConfig.getLogContext()));
    this.heartbeatMonitoringService = heartbeatMonitoringService;
    this.adaptiveThrottlingServiceStats =
        new AdaptiveThrottlingServiceStats(metricsRepository, veniceServerConfig.getClusterName());
  }

  public void registerThrottler(VeniceAdaptiveThrottler adaptiveIngestionThrottler) {
    throttlerList.add(adaptiveIngestionThrottler);
  }

  public AdaptiveThrottlingServiceStats getAdaptiveThrottlingServiceStats() {
    return adaptiveThrottlingServiceStats;
  }

  /** @return the independent p99 provider, or {@code null} when using the legacy Tehuti path. */
  public LatencyPercentileProvider getLatencyPercentileProvider() {
    return latencyPercentileProvider;
  }

  public void refreshSignalAndThrottler() {
    // Update all the signals in one shot;
    updateReadLatencySignal();
    updateHeartbeatLatencySignal();
    // Update all the throttler and record the current throttle limit
    for (VeniceAdaptiveThrottler throttler: throttlerList) {
      throttler.checkSignalAndAdjustThrottler();
    }
  }

  void updateReadLatencySignal() {
    double hostSingleGetLatencyP99;
    double hostMultiGetLatencyP99;
    double hostReadComputeLatencyP99;
    if (useSelfContainedStats) {
      hostSingleGetLatencyP99 = latencyPercentileProvider.getP99(LatencyType.SINGLE_GET);
      hostMultiGetLatencyP99 = latencyPercentileProvider.getP99(LatencyType.MULTI_GET);
      hostReadComputeLatencyP99 = latencyPercentileProvider.getP99(LatencyType.READ_COMPUTE);
      applyLatencySignals(hostSingleGetLatencyP99, hostMultiGetLatencyP99, hostReadComputeLatencyP99);
    } else {
      Metric hostSingleGetLatencyP99Metric = metricsRepository.getMetric(SINGLE_GET_LATENCY_P99_METRIC_NAME);
      Metric hostMultiGetLatencyP99Metric = metricsRepository.getMetric(MULTI_GET_LATENCY_P99_METRIC_NAME);
      Metric hostReadComputeLatencyP99Metric = metricsRepository.getMetric(READ_COMPUTE_LATENCY_P99_METRIC_NAME);
      hostSingleGetLatencyP99 = 0;
      hostMultiGetLatencyP99 = 0;
      hostReadComputeLatencyP99 = 0;

      if (hostSingleGetLatencyP99Metric != null) {
        hostSingleGetLatencyP99 = hostSingleGetLatencyP99Metric.value();
      }
      if (hostMultiGetLatencyP99Metric != null) {
        hostMultiGetLatencyP99 = hostMultiGetLatencyP99Metric.value();
      }
      if (hostReadComputeLatencyP99Metric != null) {
        hostReadComputeLatencyP99 = hostReadComputeLatencyP99Metric.value();
      }
      applyLatencySignals(hostSingleGetLatencyP99, hostMultiGetLatencyP99, hostReadComputeLatencyP99);
    }
    LOGGER.info(
        "Update read latency signal. singleGet: {} {}, multiGet: {} {}, readCompute: {} {}",
        hostSingleGetLatencyP99,
        singleGetLatencySignal,
        hostMultiGetLatencyP99,
        multiGetLatencySignal,
        hostReadComputeLatencyP99,
        readComputeLatencySignal);
  }

  private void applyLatencySignals(double sg, double mg, double rc) {
    singleGetLatencySignal = sg > singleGetLatencyP99Threshold;
    multiGetLatencySignal = mg > multiGetLatencyP99Threshold;
    readComputeLatencySignal = rc > readComputeLatencyP99Threshold;
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

  public boolean isReadLatencySignalActive() {
    return singleGetLatencySignal || multiGetLatencySignal || readComputeLatencySignal;
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
    updateService.scheduleAtFixedRate(
        this::refreshSignalAndThrottler,
        adaptiveThrottlerSignalRefreshIntervalInSeconds,
        adaptiveThrottlerSignalRefreshIntervalInSeconds,
        TimeUnit.SECONDS);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    updateService.shutdownNow();
  }

  List<VeniceAdaptiveThrottler> getThrottlerList() {
    return throttlerList;
  }

  int getAdaptiveThrottlerSignalRefreshIntervalInSeconds() {
    return adaptiveThrottlerSignalRefreshIntervalInSeconds;
  }
}
