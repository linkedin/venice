package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.participant.ParticipantMessageStoreUtils;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.stats.ParticipantStoreConsumptionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


public class ParticipantStoreConsumptionTask implements Runnable {

  private static final String CLIENT_STATS_PREFIX = "venice-client";
  private static final RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  private final Logger logger = Logger.getLogger(ParticipantStoreConsumptionTask.class);
  private final String clusterName;
  private final ParticipantStoreConsumptionStats stats;
  private final StoreIngestionService storeIngestionService;
  private final long participantMessageConsumptionDelayMs;
  private AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> participantStoreClient;

  public ParticipantStoreConsumptionTask(
      String clusterName,
      StoreIngestionService storeIngestionService,
      ParticipantStoreConsumptionStats stats,
      ClientConfig<ParticipantMessageValue> clientConfig,
      long participantMessageConsumptionDelayMs) {

    this.clusterName = clusterName;
    this.stats =  stats;
    this.storeIngestionService = storeIngestionService;
    this.participantMessageConsumptionDelayMs = participantMessageConsumptionDelayMs;

    try {
      String participantStoreName = ParticipantMessageStoreUtils.getStoreNameForCluster(clusterName);
      clientConfig.setStoreName(participantStoreName);
      clientConfig.setSpecificValueClass(ParticipantMessageValue.class);
      clientConfig.setStatsPrefix(CLIENT_STATS_PREFIX);
      this.participantStoreClient = ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    } catch (Exception e) {
      stats.recordFailedInitialization();
      logger.error("Failed to initialize participant message consumption task because participant client cannot be started", e);
    }
  }

  @Override
  public void run() {
    logger.info("Started running " + getClass().getSimpleName() + " thread for cluster: " + clusterName);
    String exceptionMessage = "Exception thrown while running " + getClass().getSimpleName() + " thread";

    for (;;) {
      try {
        Thread.sleep(participantMessageConsumptionDelayMs);

        ParticipantMessageKey key = new ParticipantMessageKey();
        key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();

        for (String topic : storeIngestionService.getIngestingTopicsWithVersionStatusNotOnline()) {
          key.resourceName = topic;

          ParticipantMessageValue value = participantStoreClient.get(key).get();
          if (value != null && value.messageType == ParticipantMessageType.KILL_PUSH_JOB.getValue()) {
            KillPushJob killPushJobMessage = (KillPushJob) value.messageUnion;
            if (storeIngestionService.killConsumptionTask(topic)) {
              // emit metrics only when a confirmed kill is made
              stats.recordKilledPushJobs();
              stats.recordKillPushJobLatency(Long.max(0,System.currentTimeMillis() - killPushJobMessage.timestamp));
            }
          }
        }

      } catch (InterruptedException e) {
        break;

      } catch (Throwable e) {
        // Some expected exception can be thrown during initializing phase of the participant store or if participant
        // store is disabled. TODO: remove logging suppression based on message once we fully move to participant store
        if (!filter.isRedundantException(e.getMessage())) {
          logger.error(exceptionMessage, e);
        }
        stats.recordKillPushJobFailedConsumption();
      }
    }

    IOUtils.closeQuietly(participantStoreClient);
    logger.info("Stopped running " + getClass().getSimpleName() + " thread for cluster: " + clusterName);
  }
}
