package com.linkedin.davinci.kafka.consumer;

import static java.lang.Thread.currentThread;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
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

  private final Map<ConsumerPoolType, EventThrottler> poolTypeRecordThrottlerMap;

  public IngestionThrottler(
      boolean isDaVinciClient,
      VeniceServerConfig serverConfig,
      Supplier<Map<String, StoreIngestionTask>> ongoingIngestionTaskMapSupplier) {
    this(
        isDaVinciClient,
        serverConfig,
        ongoingIngestionTaskMapSupplier,
        CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_INTERVAL,
        CURRENT_VERSION_BOOTSTRAPPING_DEFAULT_CHECK_TIMEUNIT);
  }

  public IngestionThrottler(
      boolean isDaVinciClient,
      VeniceServerConfig serverConfig,
      Supplier<Map<String, StoreIngestionTask>> ongoingIngestionTaskMapSupplier,
      int checkInterval,
      TimeUnit checkTimeUnit) {

    EventThrottler regularRecordThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaRecordPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_records_count",
        false,
        EventThrottler.BLOCK_STRATEGY);
    EventThrottler regularBandwidthThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaBytesPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_bandwidth",
        false,
        EventThrottler.BLOCK_STRATEGY);
    this.poolTypeRecordThrottlerMap = new VeniceConcurrentHashMap<>();
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.AA_WC_LEADER_POOL,
        new EventThrottler(
            serverConfig.getAaWCLeaderQuotaRecordsPerSecond(),
            serverConfig.getKafkaFetchQuotaTimeWindow(),
            "aa_wc_leader_records_count",
            false,
            EventThrottler.BLOCK_STRATEGY));
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
        new EventThrottler(
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
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        new EventThrottler(
            serverConfig.getCurrentVersionNonAAWCLeaderQuotaRecordsPerSecond(),
            serverConfig.getKafkaFetchQuotaTimeWindow(),
            "current_version_non_aa_wc_leader_records_count",
            false,
            EventThrottler.BLOCK_STRATEGY));
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
        new EventThrottler(
            serverConfig.getNonCurrentVersionAAWCLeaderQuotaRecordsPerSecond(),
            serverConfig.getKafkaFetchQuotaTimeWindow(),
            "non_current_version_aa_wc_leader_records_count",
            false,
            EventThrottler.BLOCK_STRATEGY));
    this.poolTypeRecordThrottlerMap.put(
        ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
        new EventThrottler(
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
      this.eventThrottlerUpdateService =
          Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("Ingestion_Event_Throttler_update"));
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
          this.finalRecordThrottler = regularRecordThrottler;
          this.finalBandwidthThrottler = regularBandwidthThrottler;
          this.isUsingSpeedupThrottler = false;
        }

      }, checkInterval, checkInterval, checkTimeUnit);
      LOGGER.info(
          "DaVinci current version ingestion speedup mode is enabled, and it will switch to speedup throttler"
              + " when there is any active current version bootstrapping");
    } else {
      this.eventThrottlerUpdateService = null;
    }

    this.finalRecordThrottler = regularRecordThrottler;
    this.finalBandwidthThrottler = regularBandwidthThrottler;
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
