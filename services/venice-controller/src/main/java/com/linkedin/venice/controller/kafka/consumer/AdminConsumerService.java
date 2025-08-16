package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.ZkAdminTopicMetadataAccessor;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;


/**
 * One consumer service for one cluster.
 */
public class AdminConsumerService extends AbstractVeniceService {
  private static final long WAITING_TIME_FOR_STOP_IN_MS = 5000;

  private final VeniceControllerClusterConfig config;
  private final VeniceHelixAdmin admin;
  private final ZkAdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final PubSubConsumerAdapterFactory consumerFactory;
  private final MetricsRepository metricsRepository;
  private final boolean remoteConsumptionEnabled;
  private final Optional<String> remoteKafkaServerUrl;
  // Only support single cluster right now
  private AdminConsumptionTask consumerTask;
  private final ThreadFactory threadFactory;
  private Thread consumerThread;

  private final PubSubTopicRepository pubSubTopicRepository;
  private final String localKafkaServerUrl;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final LogContext logContext;

  public AdminConsumerService(
      VeniceHelixAdmin admin,
      VeniceControllerClusterConfig config,
      MetricsRepository metricsRepository,
      PubSubConsumerAdapterFactory consumerFactory,
      PubSubTopicRepository pubSubTopicRepository,
      PubSubMessageDeserializer pubSubMessageDeserializer) {
    this.config = config;
    this.logContext = config.getLogContext();
    this.admin = admin;
    this.adminTopicMetadataAccessor =
        new ZkAdminTopicMetadataAccessor(admin.getZkClient(), admin.getAdapterSerializer(), config);
    this.metricsRepository = metricsRepository;
    this.remoteConsumptionEnabled = config.isAdminTopicRemoteConsumptionEnabled();
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.pubSubMessageDeserializer = pubSubMessageDeserializer;
    if (remoteConsumptionEnabled) {
      String adminTopicSourceRegion = config.getAdminTopicSourceRegion();
      remoteKafkaServerUrl = Optional.of(config.getChildDataCenterKafkaUrlMap().get(adminTopicSourceRegion));
    } else {
      remoteKafkaServerUrl = Optional.empty();
    }
    this.localKafkaServerUrl = admin.getKafkaBootstrapServers(admin.isSslToKafka());
    this.consumerFactory = consumerFactory;
    this.threadFactory = new DaemonThreadFactory("AdminConsumerService", logContext);
  }

  @Override
  public boolean startInner() throws Exception {
    String clusterName = config.getClusterName();
    consumerTask = getAdminConsumptionTaskForCluster(clusterName);
    consumerThread = threadFactory.newThread(consumerTask);
    consumerThread.start();

    return true;
  }

  @Override
  public void stopInner() throws Exception {
    if (consumerTask != null) {
      consumerTask.close();
    }
    if (consumerThread != null) {
      consumerThread.join(WAITING_TIME_FOR_STOP_IN_MS);
      if (consumerThread.isAlive()) {
        consumerThread.interrupt();
      }
    }
  }

  private AdminConsumptionTask getAdminConsumptionTaskForCluster(String clusterName) {
    return new AdminConsumptionTask(
        clusterName,
        createPubSubConsumer(clusterName),
        this.remoteConsumptionEnabled,
        remoteKafkaServerUrl,
        admin,
        adminTopicMetadataAccessor,
        admin.getExecutionIdAccessor(),
        config.isParent(),
        new AdminConsumptionStats(metricsRepository, clusterName + "-admin_consumption_task"),
        config.getAdminTopicReplicationFactor(),
        config.getMinInSyncReplicasAdminTopics(),
        config.getAdminConsumptionCycleTimeoutMs(),
        config.getAdminConsumptionMaxWorkerThreadPoolSize(),
        pubSubTopicRepository,
        config.getRegionName());
  }

  /**
   * Skip admin message with specified offset for the given cluster.
   */
  public void setOffsetToSkip(String clusterName, long offset, boolean skipDIV) {
    if (clusterName.equals(config.getClusterName())) {
      if (skipDIV) {
        consumerTask.skipMessageDIVWithOffset(offset);
      } else {
        consumerTask.skipMessageWithOffset(offset);
      }
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster " + config.getClusterName()
              + ".  Cannot skip admin message with offset " + offset + " for cluster " + clusterName);
    }
  }

  /**
   * Get the last succeeded execution id for the given cluster.
   * @param clusterName name of the Venice cluster.
   * @return last succeeded execution id for the given cluster.
   */
  public Long getLastSucceededExecutionIdInCluster(String clusterName) {
    if (clusterName.equals(config.getClusterName())) {
      return consumerTask.getLastSucceededExecutionId();
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster: " + config.getClusterName()
              + ".  Cannot get the last succeed execution Id for cluster: " + clusterName);
    }
  }

  /**
   * Get the last succeeded execution id for the given store.
   * @param storeName name of the store.
   * @return last succeeded execution id for the given store.
   */
  public Long getLastSucceededExecutionId(String storeName) {
    return consumerTask == null ? null : consumerTask.getLastSucceededExecutionId(storeName);
  }

  /**
   * Get the encountered exception during admin message consumption for the given store.
   * @param storeName name of the store.
   * @return last encountered exception.
   */
  public Exception getLastExceptionForStore(String storeName) {
    return consumerTask == null ? null : consumerTask.getLastExceptionForStore(storeName);
  }

  /**
   * @return The first or the smallest failing offset.
   */
  public long getFailingOffset() {
    return consumerTask.getFailingOffset();
  }

  /**
   * @return cluster-level execution id, offset, and upstream offset in a child colo.
   */
  public AdminMetadata getAdminTopicMetadata(String clusterName) {
    if (clusterName.equals(config.getClusterName())) {
      return adminTopicMetadataAccessor.getMetadata(clusterName);
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster: " + config.getClusterName()
              + ".  Cannot get the last succeed execution Id for cluster: " + clusterName);
    }
  }

  /**
   * Update cluster-level execution id, offset, and upstream offset in a child colo.
   */
  public void updateAdminTopicMetadata(String clusterName, long executionId, long offset, long upstreamOffset) {
    if (clusterName.equals(config.getClusterName())) {
      try (AutoCloseableLock ignore =
          admin.getHelixVeniceClusterResources(clusterName).getClusterLockManager().createClusterWriteLock()) {
        Map<String, Long> metadata = AdminTopicMetadataAccessor.generateMetadataMap(
            Optional.of(offset),
            Optional.of(upstreamOffset),
            Optional.of(executionId),
            Optional.empty());
        adminTopicMetadataAccessor.updateMetadata(clusterName, AdminMetadata.fromLegacyMap(metadata));
      }
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster: " + config.getClusterName()
              + ".  Cannot get the last succeed execution Id for cluster: " + clusterName);
    }
  }

  /**
   * Update the admin operation protocol version for the given cluster.
   */
  public void updateAdminOperationProtocolVersion(String clusterName, long adminOperationProtocolVersion) {
    if (clusterName.equals(config.getClusterName())) {
      try (AutoCloseableLock ignore =
          admin.getHelixVeniceClusterResources(clusterName).getClusterLockManager().createClusterWriteLock()) {
        Map<String, Long> metadata = AdminTopicMetadataAccessor.generateMetadataMap(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(adminOperationProtocolVersion));
        adminTopicMetadataAccessor.updateMetadata(clusterName, AdminMetadata.fromLegacyMap(metadata));
      }
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster: " + config.getClusterName()
              + ".  Cannot update the version for cluster: " + clusterName);
    }
  }

  private PubSubConsumerAdapter createPubSubConsumer(String clusterName) {
    String pubSubServerUrl = remoteConsumptionEnabled ? remoteKafkaServerUrl.get() : localKafkaServerUrl;
    Properties kafkaConsumerProperties = admin.getPubSubSSLProperties(pubSubServerUrl).toProperties();
    PubSubConsumerAdapterContext pubSubConsumerAdapterContext = new PubSubConsumerAdapterContext.Builder()
        .setConsumerName("admin-channel-consumer-for-" + clusterName + (logContext != null ? "-" + logContext : ""))
        .setVeniceProperties(new VeniceProperties(kafkaConsumerProperties))
        .setPubSubMessageDeserializer(pubSubMessageDeserializer)
        .setPubSubPositionTypeRegistry(config.getPubSubPositionTypeRegistry())
        .setPubSubTopicRepository(pubSubTopicRepository)
        .build();
    return consumerFactory.create(pubSubConsumerAdapterContext);
  }

  // For testing only.
  public PubSubMessageDeserializer getDeserializer() {
    return this.pubSubMessageDeserializer;
  }
}
