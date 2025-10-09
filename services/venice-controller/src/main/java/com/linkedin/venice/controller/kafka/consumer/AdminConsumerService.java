package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.pubsub.PubSubUtil.getPubSubPositionWireFormat;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.ZkAdminTopicMetadataAccessor;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.PubSubPositionGrpcWireFormat;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
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
  private final PubSubPositionDeserializer pubSubPositionDeserializer;

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
    this.pubSubPositionDeserializer = config.getPubSubPositionDeserializer();
    this.threadFactory = new DaemonThreadFactory("AdminConsumerService-" + config.getClusterName(), logContext);
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
  public void setPositionToSkip(String clusterName, PubSubPosition position, boolean skipDIV) {
    if (clusterName.equals(config.getClusterName())) {
      if (skipDIV) {
        consumerTask.skipMessageDIVWithPosition(position);
      } else {
        consumerTask.skipMessageWithPosition(position);
      }
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster " + config.getClusterName()
              + ". Cannot skip admin message with position " + position + " for cluster " + clusterName);
    }
  }

  public void setExecutionIdToSkip(String clusterName, long executionId, boolean skipDIV) {
    if (clusterName.equals(config.getClusterName())) {
      if (skipDIV) {
        consumerTask.skipMessageDIVWithExecutionId(executionId);
      } else {
        consumerTask.skipMessageWithExecutionId(executionId);
      }
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster " + config.getClusterName()
              + ".  Cannot skip admin message with execution ID " + executionId + " for cluster " + clusterName);
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
   * @return The first or the smallest failing position.
   */
  @VisibleForTesting
  public PubSubPosition getFailingPosition() {
    return consumerTask.getFailingPosition();
  }

  /**
   * @return The first or the smallest failing execution id.
   */
  @VisibleForTesting
  public long getFailingExecutionId() {
    return consumerTask.getFailingExecutionId();
  }

  /**
   * @return cluster-level execution id, position, and upstream position in a child colo.
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
   * Update cluster-level execution id, position, and upstream position in a child colo.
   */
  public void updateAdminTopicMetadata(
      String clusterName,
      long executionId,
      PubSubPositionGrpcWireFormat position,
      PubSubPositionGrpcWireFormat upstreamPosition) {
    if (clusterName.equals(config.getClusterName())) {
      try (AutoCloseableLock ignore =
          admin.getHelixVeniceClusterResources(clusterName).getClusterLockManager().createClusterWriteLock()) {

        AdminMetadata metadata = new AdminMetadata();
        metadata.setPubSubPosition(pubSubPositionDeserializer.toPosition(getPubSubPositionWireFormat(position)));
        metadata.setUpstreamPubSubPosition(
            pubSubPositionDeserializer.toPosition(getPubSubPositionWireFormat(upstreamPosition)));
        metadata.setExecutionId(executionId);
        adminTopicMetadataAccessor.updateMetadata(clusterName, metadata);
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
        AdminMetadata metadata = new AdminMetadata();
        metadata.setAdminOperationProtocolVersion(adminOperationProtocolVersion);
        adminTopicMetadataAccessor.updateMetadata(clusterName, metadata);
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
