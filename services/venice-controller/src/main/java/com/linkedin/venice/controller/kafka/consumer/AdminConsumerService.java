package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_AUTO_OFFSET_RESET_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLIENT_ID_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_ENABLE_AUTO_COMMIT_CONFIG;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.ZkAdminTopicMetadataAccessor;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.VeniceProperties;
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
  private final ThreadFactory threadFactory = new DaemonThreadFactory("AdminTopicConsumer");
  private Thread consumerThread;

  private final PubSubTopicRepository pubSubTopicRepository;
  private final String localKafkaServerUrl;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;

  public AdminConsumerService(
      VeniceHelixAdmin admin,
      VeniceControllerClusterConfig config,
      MetricsRepository metricsRepository,
      PubSubTopicRepository pubSubTopicRepository,
      PubSubMessageDeserializer pubSubMessageDeserializer) {
    this.config = config;
    this.admin = admin;
    this.adminTopicMetadataAccessor =
        new ZkAdminTopicMetadataAccessor(admin.getZkClient(), admin.getAdapterSerializer());
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
    this.consumerFactory = admin.getPubSubConsumerAdapterFactory();
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
        createKafkaConsumer(clusterName),
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
  public Map<String, Long> getAdminTopicMetadata(String clusterName) {
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
      Map<String, Long> metadata = AdminTopicMetadataAccessor.generateMetadataMap(offset, upstreamOffset, executionId);
      adminTopicMetadataAccessor.updateMetadata(clusterName, metadata);
    } else {
      throw new VeniceException(
          "This AdminConsumptionService is for cluster: " + config.getClusterName()
              + ".  Cannot get the last succeed execution Id for cluster: " + clusterName);
    }
  }

  private PubSubConsumerAdapter createKafkaConsumer(String clusterName) {
    String pubSubServerUrl = remoteConsumptionEnabled ? remoteKafkaServerUrl.get() : localKafkaServerUrl;
    Properties kafkaConsumerProperties = admin.getPubSubSSLProperties(pubSubServerUrl).toProperties();
    /**
     * {@link KAFKA_CLIENT_ID_CONFIG} can be used to identify different consumers while checking Kafka related metrics.
     */
    kafkaConsumerProperties.setProperty(KAFKA_CLIENT_ID_CONFIG, clusterName);
    kafkaConsumerProperties.setProperty(KAFKA_AUTO_OFFSET_RESET_CONFIG, "earliest");
    /**
     * Reason to disable auto_commit
     * 1. {@link AdminConsumptionTask} is persisting {@link com.linkedin.venice.offsets.OffsetRecord} in Zookeeper.
     */
    kafkaConsumerProperties.setProperty(KAFKA_ENABLE_AUTO_COMMIT_CONFIG, "false");
    return consumerFactory
        .create(new VeniceProperties(kafkaConsumerProperties), false, pubSubMessageDeserializer, clusterName);
  }

  // For testing only.
  public PubSubMessageDeserializer getDeserializer() {
    return this.pubSubMessageDeserializer;
  }
}
