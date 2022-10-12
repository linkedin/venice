package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.ParticipantStoreConsumptionStats;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ParticipantStoreConsumptionTask implements Runnable, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(ParticipantStoreConsumptionTask.class);

  private static final String CLIENT_STATS_PREFIX = "venice-client";
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final AtomicBoolean isClosing = new AtomicBoolean();
  private final ParticipantStoreConsumptionStats stats;
  private final StoreIngestionService storeIngestionService;
  private final long participantMessageConsumptionDelayMs;
  private final ClusterInfoProvider clusterInfoProvider;
  private final ClientConfig<ParticipantMessageValue> clientConfig;
  private final Map<String, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> clientMap =
      new VeniceConcurrentHashMap<>();
  private final ICProvider icProvider;

  public ParticipantStoreConsumptionTask(
      StoreIngestionService storeIngestionService,
      ClusterInfoProvider clusterInfoProvider,
      ParticipantStoreConsumptionStats stats,
      ClientConfig<ParticipantMessageValue> clientConfig,
      long participantMessageConsumptionDelayMs,
      ICProvider icProvider) {

    this.stats = Validate.notNull(stats);
    this.storeIngestionService = Validate.notNull(storeIngestionService);
    this.clusterInfoProvider = Validate.notNull(clusterInfoProvider);
    this.clientConfig = Validate.notNull(clientConfig);
    this.participantMessageConsumptionDelayMs = participantMessageConsumptionDelayMs;
    this.icProvider = icProvider;
  }

  @Override
  public void run() {
    LOGGER.info("Started running {}", getClass().getSimpleName());
    while (!isClosing.get() && !Thread.currentThread().isInterrupted()) {
      stats.recordHeartbeat();
      try {
        Thread.sleep(participantMessageConsumptionDelayMs);

        for (String topic: storeIngestionService.getIngestingTopicsWithVersionStatusNotOnline()) {
          try {
            ParticipantMessageKey key = new ParticipantMessageKey();
            key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
            key.resourceName = topic;
            String clusterName = clusterInfoProvider.getVeniceCluster(Version.parseStoreFromKafkaTopicName(topic));
            if (clusterName == null) {
              continue;
            }

            ParticipantMessageValue value;
            if (icProvider != null) {
              CompletableFuture<ParticipantMessageValue> future = icProvider
                  .call(this.getClass().getCanonicalName(), () -> getParticipantStoreClient(clusterName).get(key));
              value = future.get();
            } else {
              value = getParticipantStoreClient(clusterName).get(key).get();
            }

            if (value != null && value.messageType == ParticipantMessageType.KILL_PUSH_JOB.getValue()) {
              KillPushJob killPushJobMessage = (KillPushJob) value.messageUnion;
              if (storeIngestionService.killConsumptionTask(topic)) {
                // emit metrics only when a confirmed kill is made
                stats.recordKilledPushJobs();
                stats.recordKillPushJobLatency(Long.max(0, System.currentTimeMillis() - killPushJobMessage.timestamp));
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Got an InterruptedException while killing consumption task for topic: {}", topic, e);
            throw e;
          } catch (Exception e) {
            String msg = "Unexpected exception while trying to check or kill ingestion topic: " + topic + ". ExMsg: "
                + e.getMessage();
            if (!EXCEPTION_FILTER.isRedundantException(msg)) {
              LOGGER.error(msg, e);
            }
            stats.recordKillPushJobFailedConsumption();
          }
        }
      } catch (InterruptedException e) {
        LOGGER.info("ParticipantStoreConsumptionTask was interrupted and hence exiting now...", e);
        break;
      } catch (Exception e) {
        // Some expected exception can be thrown during initializing phase of the participant store
        // or if participant store is disabled.
        String msg = "Exception thrown while running " + getClass().getSimpleName() + " thread. ExMsg: "
            + ExceptionUtils.compactExceptionDescription(e);
        if (!EXCEPTION_FILTER.isRedundantException(msg)) {
          LOGGER.error(msg, e);
        }
        stats.recordKillPushJobFailedConsumption();
      } catch (Throwable t) {
        LOGGER.error("Throwable thrown while running {} thread", getClass().getSimpleName(), t);
        break;
      }
    }

    LOGGER.info("Stopped running {}", getClass().getSimpleName());
  }

  private AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> getParticipantStoreClient(
      String clusterName) {
    try {
      clientMap.computeIfAbsent(clusterName, k -> {
        ClientConfig<ParticipantMessageValue> newClientConfig = ClientConfig.cloneConfig(clientConfig)
            .setStoreName(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName))
            .setSpecificValueClass(ParticipantMessageValue.class)
            .setStatsPrefix(CLIENT_STATS_PREFIX);
        return ClientFactory.getAndStartSpecificAvroClient(newClientConfig);
      });
    } catch (Exception e) {
      stats.recordFailedInitialization();
      LOGGER.error("Failed to get participant client for cluster: {}", clusterName, e);
    }
    return clientMap.get(clusterName);
  }

  @Override
  public void close() {
    isClosing.set(true);
    clientMap.values().forEach(AvroGenericStoreClient::close);
  }
}
