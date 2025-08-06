package com.linkedin.davinci.kafka.consumer;

import static java.lang.Thread.currentThread;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This throttler has the following functionality:
 * 1. When running in DaVinci mode, if there are active current version bootstrapping with speedup mode is on, this
 *    ingestion throttler will switch to speedup throttler.
 * 2. Otherwise, this class will switch to regular throttler.
 *
 * This throttler is supposed to be adaptive throttler to speed up the DaVinci bootstrapping and fall back to the regular
 * mode when the bootstrapping is done.
 */
public class IngestionThrottler implements Closeable {
  private final Logger LOGGER = LogManager.getLogger(IngestionThrottler.class);

  private final static int CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_INTERVAL = 30;
  private final static TimeUnit CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_TIMEUNIT = TimeUnit.SECONDS;

  private final ScheduledExecutorService eventThrottlerUpdateService;

  private volatile EventThrottler finalRecordThrottler;
  private volatile EventThrottler finalBandwidthThrottler;
  private boolean isUsingSpeedupThrottler = false;

  private final EnumMap<ConsumerPoolType, EventThrottler> poolTypeRecordThrottlerMap;

  public IngestionThrottler(
      boolean isDaVinciClient,
      VeniceServerConfig serverConfig,
      Supplier<Map<String, StoreIngestionTask>> ongoingIngestionTaskMapSupplier,
      AdaptiveThrottlerSignalService adaptiveThrottlerSignalService) {
    this(
        isDaVinciClient,
        serverConfig,
        ongoingIngestionTaskMapSupplier,
        CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_INTERVAL,
        CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_TIMEUNIT,
        adaptiveThrottlerSignalService);
  }

  public IngestionThrottler(
      boolean isDaVinciClient,
      VeniceServerConfig serverConfig,
      Supplier<Map<String, StoreIngestionTask>> ongoingIngestionTaskMapSupplier,
      int checkInterval,
      TimeUnit checkTimeUnit,
      AdaptiveThrottlerSignalService adaptiveThrottlerSignalService) {
    VeniceAdaptiveIngestionThrottler globalRecordAdaptiveIngestionThrottler;
    EventThrottler globalRecordThrottler;
    EventThrottler globalBandwidthThrottler;
    VeniceAdaptiveIngestionThrottler globalBandwidthAdaptiveIngestionThrottler;
    boolean isAdaptiveThrottlerEnabled = serverConfig.isAdaptiveThrottlerEnabled();

    if (isAdaptiveThrottlerEnabled) {
      globalRecordThrottler = null;
      globalRecordAdaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getKafkaFetchQuotaRecordPerSecond(),
          serverConfig.getKafkaFetchThrottlerFactorsPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "kafka_consumption_records_count");
      globalRecordAdaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveThrottlerSignalService.registerThrottler(globalRecordAdaptiveIngestionThrottler);
      globalBandwidthThrottler = null;
      globalBandwidthAdaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getKafkaFetchQuotaBytesPerSecond(),
          serverConfig.getKafkaFetchThrottlerFactorsPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "kafka_consumption_bandwidth");
      globalBandwidthAdaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveThrottlerSignalService.registerThrottler(globalBandwidthAdaptiveIngestionThrottler);
    } else {
      globalRecordAdaptiveIngestionThrottler = null;
      globalRecordThrottler = new EventThrottler(
          serverConfig.getKafkaFetchQuotaRecordPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "kafka_consumption_records_count",
          false,
          EventThrottler.BLOCK_STRATEGY);
      globalBandwidthAdaptiveIngestionThrottler = null;
      globalBandwidthThrottler = new EventThrottler(
          serverConfig.getKafkaFetchQuotaBytesPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "kafka_consumption_bandwidth",
          false,
          EventThrottler.BLOCK_STRATEGY);
    }
    this.poolTypeRecordThrottlerMap = new EnumMap<>(ConsumerPoolType.class);
    VeniceAdaptiveIngestionThrottler adaptiveIngestionThrottler = null;
    if (isAdaptiveThrottlerEnabled) {
      adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getAaWCLeaderQuotaRecordsPerSecond(),
          serverConfig.getThrottlerFactorsForAAWCLeader(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "aa_wc_leader_records_count");
      adaptiveIngestionThrottler.registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    }
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.AA_WC_LEADER_POOL,
        isAdaptiveThrottlerEnabled
            ? adaptiveIngestionThrottler
            : new EventThrottler(
                serverConfig.getAaWCLeaderQuotaRecordsPerSecond(),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "aa_wc_leader_records_count",
                false,
                EventThrottler.BLOCK_STRATEGY));
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.SEP_RT_LEADER_POOL,
        new EventThrottler(
            serverConfig.getSepRTLeaderQuotaRecordsPerSecond(),
            serverConfig.getKafkaFetchQuotaTimeWindow(),
            "sep_rt_leader_records_count",
            false,
            EventThrottler.BLOCK_STRATEGY));
    if (isAdaptiveThrottlerEnabled) {
      adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getCurrentVersionAAWCLeaderQuotaRecordsPerSecond(),
          serverConfig.getThrottlerFactorsForCurrentVersionAAWCLeader(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "current_version_aa_wc_leader_records_count");
      adaptiveIngestionThrottler.registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    }
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
        isAdaptiveThrottlerEnabled
            ? adaptiveIngestionThrottler
            : new EventThrottler(
                serverConfig.getCurrentVersionAAWCLeaderQuotaRecordsPerSecond(),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "current_version_aa_wc_leader_records_count",
                false,
                EventThrottler.BLOCK_STRATEGY));
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.CURRENT_VERSION_SEP_RT_LEADER_POOL,
        new EventThrottler(
            serverConfig.getCurrentVersionSepRTLeaderQuotaRecordsPerSecond(),
            serverConfig.getKafkaFetchQuotaTimeWindow(),
            "current_version_sep_rt_leader_records_count",
            false,
            EventThrottler.BLOCK_STRATEGY));
    if (isAdaptiveThrottlerEnabled) {
      adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond(),
          serverConfig.getThrottlerFactorsForCurrentVersionNonAAWCLeader(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "current_version_non_aa_wc_leader_records_count");
      adaptiveIngestionThrottler.registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    }
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        isAdaptiveThrottlerEnabled
            ? adaptiveIngestionThrottler
            : new EventThrottler(
                serverConfig.getCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond(),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "current_version_non_aa_wc_leader_records_count",
                false,
                EventThrottler.BLOCK_STRATEGY));
    if (isAdaptiveThrottlerEnabled) {
      adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getNonCurrentVersionAAWCLeaderQuotaRecordsPerSecond(),
          serverConfig.getThrottlerFactorsForNonCurrentVersionAAWCLeader(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "non_current_version_aa_wc_leader_records_count");
      adaptiveIngestionThrottler.registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isCurrentLeaderMaxHeartbeatLagSignalActive);
      adaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isCurrentFollowerMaxHeartbeatLagSignalActive);
      adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    }
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
        isAdaptiveThrottlerEnabled
            ? adaptiveIngestionThrottler
            : new EventThrottler(
                serverConfig.getNonCurrentVersionAAWCLeaderQuotaRecordsPerSecond(),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "non_current_version_aa_wc_leader_records_count",
                false,
                EventThrottler.BLOCK_STRATEGY));
    if (isAdaptiveThrottlerEnabled) {
      adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(
          serverConfig.getAdaptiveThrottlerSignalIdleThreshold(),
          serverConfig.getNonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond(),
          serverConfig.getThrottlerFactorsForNonCurrentVersionAAWCLeader(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "non_current_version_non_aa_wc_leader_records_count");
      adaptiveIngestionThrottler.registerLimiterSignal(adaptiveThrottlerSignalService::isReadLatencySignalActive);
      adaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isCurrentLeaderMaxHeartbeatLagSignalActive);
      adaptiveIngestionThrottler
          .registerLimiterSignal(adaptiveThrottlerSignalService::isCurrentFollowerMaxHeartbeatLagSignalActive);
      adaptiveThrottlerSignalService.registerThrottler(adaptiveIngestionThrottler);
    }
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        isAdaptiveThrottlerEnabled
            ? adaptiveIngestionThrottler
            : new EventThrottler(
                serverConfig.getNonCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond(),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "non_current_version_non_aa_wc_leader_records_count",
                false,
                EventThrottler.BLOCK_STRATEGY));

    if (isDaVinciClient && serverConfig.isDaVinciCurrentVersionBootstrappingSpeedupEnabled()) {
      EventThrottler speedupRecordThrottler = new EventThrottler(
          serverConfig.getDaVinciCurrentVersionBootstrappingQuotaRecordsPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "current_version_speedup_kafka_consumption_records_count",
          false,
          EventThrottler.BLOCK_STRATEGY);
      EventThrottler speedupBandwidthThrottler = new EventThrottler(
          serverConfig.getDaVinciCurrentVersionBootstrappingQuotaBytesPerSecond(),
          serverConfig.getKafkaFetchQuotaTimeWindow(),
          "current_version_speedup_kafka_consumption_bandwidth",
          false,
          EventThrottler.BLOCK_STRATEGY);
      this.eventThrottlerUpdateService = Executors.newSingleThreadScheduledExecutor(
          new DaemonThreadFactory("Ingestion_Event_Throttler_update", serverConfig.getLogContext()));
      this.eventThrottlerUpdateService.scheduleAtFixedRate(() -> {
        Map<String, StoreIngestionTask> ongoingStoreIngestionTaskMap = ongoingIngestionTaskMapSupplier.get();
        boolean hasCurrentVersionBootstrapping = false;
        String topicOfCurrentVersionBootstrapping = "";
        for (Map.Entry<String, StoreIngestionTask> entry: ongoingStoreIngestionTaskMap.entrySet()) {
          StoreIngestionTask task = entry.getValue();
          if (task.isCurrentVersion() && !task.hasAllPartitionReportedCompleted()) {
            hasCurrentVersionBootstrapping = true;
            topicOfCurrentVersionBootstrapping = entry.getKey();
            break;
          }
        }
        if (hasCurrentVersionBootstrapping && !isUsingSpeedupThrottler) {
          LOGGER.info(
              "Found one current version bootstrapping: {}, will switch to speedup throttler",
              topicOfCurrentVersionBootstrapping);
          this.finalRecordThrottler = speedupRecordThrottler;
          this.finalBandwidthThrottler = speedupBandwidthThrottler;
          this.isUsingSpeedupThrottler = true;
        } else if (!hasCurrentVersionBootstrapping && isUsingSpeedupThrottler) {
          LOGGER.info("There is no active current version bootstrapping, so switch to regular throttler");
          this.finalRecordThrottler = globalRecordThrottler;
          this.finalBandwidthThrottler = globalBandwidthThrottler;
          this.isUsingSpeedupThrottler = false;
        }

      }, checkInterval, checkInterval, checkTimeUnit);
      LOGGER.info(
          "DaVinci current version ingestion speedup mode is enabled, and it will switch to speedup throttler"
              + " when there is any active current version bootstrapping");
    } else {
      this.eventThrottlerUpdateService = null;
    }

    this.finalRecordThrottler =
        isAdaptiveThrottlerEnabled ? globalRecordAdaptiveIngestionThrottler : globalRecordThrottler;
    this.finalBandwidthThrottler =
        isAdaptiveThrottlerEnabled ? globalBandwidthAdaptiveIngestionThrottler : globalBandwidthThrottler;
  }

  public void maybeThrottleRecordRate(ConsumerPoolType poolType, int count) {
    EventThrottler poolTypeRecordThrottler = poolTypeRecordThrottlerMap.get(poolType);
    if (poolTypeRecordThrottler != null) {
      poolTypeRecordThrottler.maybeThrottle(count);
    }
    finalRecordThrottler.maybeThrottle(count);
  }

  public void maybeThrottleBandwidth(int totalBytes) {
    finalBandwidthThrottler.maybeThrottle(totalBytes);
  }

  public boolean isUsingSpeedupThrottler() {
    return isUsingSpeedupThrottler;
  }

  @Override
  public void close() throws IOException {
    if (eventThrottlerUpdateService != null) {
      eventThrottlerUpdateService.shutdownNow();
      try {
        if (!eventThrottlerUpdateService.awaitTermination(10, TimeUnit.SECONDS)) {
          LOGGER.error("Failed to shutdown executor for {}", this.getClass().getSimpleName());
        }
      } catch (InterruptedException e) {
        LOGGER.error("Got interrupted when shutting down the scheduler for {}", this.getClass().getSimpleName());
        currentThread().interrupt();
      }
    }
  }

  // For test
  void setupRecordThrottlerForPoolType(ConsumerPoolType poolType, EventThrottler throttler) {
    poolTypeRecordThrottlerMap.put(poolType, throttler);
  }

  void setupGlobalRecordThrottler(EventThrottler globalRecordThrottler) {
    this.finalRecordThrottler = globalRecordThrottler;
  }
}
