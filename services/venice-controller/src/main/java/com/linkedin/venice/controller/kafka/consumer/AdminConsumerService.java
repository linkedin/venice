package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.ZkAdminTopicMetadataAccessor;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.KafkaClientFactory.MetricsParameters;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * One consumer service for one cluster.
 */
public class AdminConsumerService extends AbstractVeniceService {
  private static final long WAITING_TIME_FOR_STOP_IN_MS = 5000;

  private final VeniceControllerConfig config;
  private final VeniceHelixAdmin admin;
  private final ZkAdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final KafkaClientFactory consumerFactory;
  private final MetricsRepository metricsRepository;
  private final boolean remoteConsumptionEnabled;
  private final Optional<String> remoteKafkaServerUrl;
  private final Optional<String> remoteKafkaZkAddress;
  // Only support single cluster right now
  private AdminConsumptionTask consumerTask;
  private final ThreadFactory threadFactory = new DaemonThreadFactory("AdminTopicConsumer");
  private Thread consumerThread;

  private final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;

  public AdminConsumerService(
      String cluster,
      VeniceHelixAdmin admin,
      VeniceControllerConfig config,
      MetricsRepository metricsRepository,
      Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader) {
    this.config = config;
    this.admin = admin;
    this.adminTopicMetadataAccessor =
        new ZkAdminTopicMetadataAccessor(admin.getZkClient(), admin.getAdapterSerializer());
    this.metricsRepository = metricsRepository;
    this.remoteConsumptionEnabled = config.isAdminTopicRemoteConsumptionEnabled();
    if (remoteConsumptionEnabled) {
      String adminTopicSourceRegion = config.getAdminTopicSourceRegion();
      remoteKafkaServerUrl = Optional.of(config.getChildDataCenterKafkaUrlMap().get(adminTopicSourceRegion));
      remoteKafkaZkAddress = Optional.of(config.getChildDataCenterKafkaZkMap().get(adminTopicSourceRegion));
      Optional<MetricsParameters> metricsParameters = Optional.of(
          new MetricsParameters(
              admin.getVeniceConsumerFactory().getClass(),
              this.getClass(),
              cluster + "_" + remoteKafkaServerUrl.get(),
              metricsRepository));
      this.consumerFactory = admin.getVeniceConsumerFactory()
          .clone(remoteKafkaServerUrl.get(), remoteKafkaZkAddress.get(), metricsParameters);
    } else {
      this.consumerFactory = admin.getVeniceConsumerFactory();
      remoteKafkaServerUrl = Optional.empty();
      remoteKafkaZkAddress = Optional.empty();
    }
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;
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
        remoteKafkaZkAddress,
        admin,
        adminTopicMetadataAccessor,
        admin.getExecutionIdAccessor(),
        config.isParent(),
        new AdminConsumptionStats(metricsRepository, clusterName + "-admin_consumption_task"),
        config.getAdminTopicReplicationFactor(),
        config.getMinInSyncReplicasAdminTopics(),
        config.getAdminConsumptionCycleTimeoutMs(),
        config.getAdminConsumptionMaxWorkerThreadPoolSize());
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

  private KafkaConsumerWrapper createKafkaConsumer(String clusterName) {
    Properties kafkaConsumerProperties = new Properties();
    /**
     * {@link ConsumerConfig.CLIENT_ID_CONFIG} can be used to identify different consumers while checking Kafka related metrics.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clusterName);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    /**
     * Reason to disable auto_commit
     * 1. {@link AdminConsumptionTask} is persisting {@link com.linkedin.venice.offsets.OffsetRecord} in Zookeeper.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // This is a temporary fix for the issue described here
    // https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    // In our case "com.linkedin.venice.serialization.KafkaKeySerializer" class can not be found
    // because class loader has no venice-common in class path. This can be only reproduced on JDK11
    // Trying to avoid class loading via Kafka's ConfigDef class
    kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);

    if (kafkaMessageEnvelopeSchemaReader.isPresent()) {
      kafkaConsumerProperties
          .put(InternalAvroSpecificSerializer.VENICE_SCHEMA_READER_CONFIG, kafkaMessageEnvelopeSchemaReader.get());
    }
    return consumerFactory.getConsumer(kafkaConsumerProperties);
  }
}
