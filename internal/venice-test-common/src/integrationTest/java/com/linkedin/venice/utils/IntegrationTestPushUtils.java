package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.kafka.consumer.ConsumerPoolType;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.TopicPartitionIngestionInfo;
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.DaVinciClientDiskFullTest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.integration.utils.KafkaTestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;


public class IntegrationTestPushUtils {
  private static final Logger LOGGER = LogManager.getLogger(IntegrationTestPushUtils.class);

  private static final VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> VENICE_JSON_SERIALIZER =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
      });

  public static Properties defaultVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Map<String, String> childRegionNamesToZkAddress =
        Collections.singletonMap(veniceCluster.getRegionName(), veniceCluster.getZk().getAddress());
    return TestWriteUtils.defaultVPJPropsWithD2Routing(
        null,
        null,
        childRegionNamesToZkAddress,
        VeniceControllerWrapper.PARENT_D2_SERVICE_NAME,
        VeniceControllerWrapper.D2_SERVICE_NAME,
        inputDirPath,
        storeName);
  }

  public static Properties defaultVPJPropsWithoutD2Routing(
      VeniceClusterWrapper veniceCluster,
      String inputDirPath,
      String storeName) {
    return TestWriteUtils.defaultVPJProps(veniceCluster.getAllControllersURLs(), inputDirPath, storeName);
  }

  public static Properties defaultVPJProps(
      VeniceMultiClusterWrapper veniceMultiCluster,
      String inputDirPath,
      String storeName) {
    Map<String, String> childRegionNamesToZkAddress = Collections
        .singletonMap(veniceMultiCluster.getRegionName(), veniceMultiCluster.getZkServerWrapper().getAddress());
    return TestWriteUtils.defaultVPJPropsWithD2Routing(
        null,
        null,
        childRegionNamesToZkAddress,
        VeniceControllerWrapper.PARENT_D2_SERVICE_NAME,
        VeniceControllerWrapper.D2_SERVICE_NAME,
        inputDirPath,
        storeName);
  }

  public static Properties defaultVPJProps(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper,
      String inputDirPath,
      String storeName) {
    String parentRegionZkAddress = multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress();
    String parentRegionName = multiRegionMultiClusterWrapper.getParentRegionName();

    Map<String, String> childRegionNamesToZkAddress = multiRegionMultiClusterWrapper.getChildRegions()
        .stream()
        .collect(
            Collectors.toMap(
                veniceRegion -> veniceRegion.getRegionName(),
                veniceRegion -> veniceRegion.getZkServerWrapper().getAddress()));
    return TestWriteUtils.defaultVPJPropsWithD2Routing(
        parentRegionName,
        parentRegionZkAddress,
        childRegionNamesToZkAddress,
        VeniceControllerWrapper.PARENT_D2_SERVICE_NAME,
        VeniceControllerWrapper.D2_SERVICE_NAME,
        inputDirPath,
        storeName);
  }

  public static Properties sslVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.putAll(KafkaTestUtils.getLocalKafkaClientSSLConfig());
    return props;
  }

  /**
   * Blocking, waits for new version to go online
   */
  public static void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    runVPJ(vpjProperties, expectedVersionNumber, controllerClient, Optional.empty());
  }

  public static void runVPJ(
      Properties vpjProperties,
      int expectedVersionNumber,
      ControllerClient controllerClient,
      Optional<DaVinciClientDiskFullTest.SentPushJobDetailsTrackerImpl> pushJobDetailsTracker) {
    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("hybrid-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      pushJobDetailsTracker.ifPresent(job::setSentPushJobDetailsTracker);
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          5,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
      LOGGER.info("**TIME** VPJ #{} takes {} ms", expectedVersionNumber, (System.currentTimeMillis() - vpjStart));
    }
  }

  public static ControllerClient createStoreForJob(
      VeniceClusterWrapper veniceClusterWrapper,
      String keySchemaStr,
      String valueSchema,
      Properties props) {
    return createStoreForJob(
        veniceClusterWrapper.getClusterName(),
        keySchemaStr,
        valueSchema,
        props,
        CompressionStrategy.NO_OP,
        false,
        false);
  }

  public static void makeStoreHybrid(
      VeniceClusterWrapper venice,
      String storeName,
      long rewindSeconds,
      long offsetLag) {
    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(venice.getClusterName(), venice.getRandomRouterURL())) {
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(rewindSeconds).setHybridOffsetLagThreshold(offsetLag));
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    }
  }

  public static Map<String, String> getSamzaProducerConfig(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, type.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, venice.getZk().getAddress());
    samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, VeniceControllerWrapper.D2_SERVICE_NAME);
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_zk_address");
    samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, "invalid_parent_d2_service");
    samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    samzaConfig.putAll(
        PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
    return samzaConfig;
  }

  /**
   *  Create Samza Producer config for multi cluster setup.
   */
  public static Map<String, String> getSamzaProducerConfig(
      List<VeniceMultiClusterWrapper> dataCenters,
      int index,
      String storeName) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
    samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, dataCenters.get(index).getZkServerWrapper().getAddress());
    samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_d2_service");
    samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
    samzaConfig.put(DEPLOYMENT_ID, "DC_" + index + "_" + storeName);
    samzaConfig.put(SSL_ENABLED, "false");
    return samzaConfig;
  }

  @SafeVarargs
  public static SystemProducer getSamzaProducer(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type,
      Pair<String, String>... optionalConfigs) {
    Map<String, String> samzaConfig = getSamzaProducerConfig(venice, storeName, type);
    for (Pair<String, String> config: optionalConfigs) {
      samzaConfig.put(config.getFirst(), config.getSecond());
    }
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    veniceProducer.start();
    return veniceProducer;
  }

  public static String getKeySchemaString(Schema recordSchema, Properties props) {
    return getSchemaString(recordSchema, props, KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
  }

  public static String getValueSchemaString(Schema recordSchema, Properties props) {
    return getSchemaString(recordSchema, props, VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
  }

  private static String getSchemaString(Schema recordSchema, Properties props, String field, String fallbackField) {
    return recordSchema.getField(props.getProperty(field, fallbackField)).schema().toString();
  }

  /**
   * Consider using {@link VeniceClusterWrapper#createStoreForJob(Schema, Properties)} if you do not need the controller
   * client returned by this function.
   */
  public static ControllerClient createStoreForJob(String veniceClusterName, Schema recordSchema, Properties props) {
    return createStoreForJob(
        veniceClusterName,
        getKeySchemaString(recordSchema, props),
        getValueSchemaString(recordSchema, props),
        props,
        CompressionStrategy.NO_OP,
        false,
        false);
  }

  public static ControllerClient createStoreForJob(
      String veniceClusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      CompressionStrategy compressionStrategy,
      boolean chunkingEnabled,
      boolean incrementalPushEnabled) {

    UpdateStoreQueryParams storeParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setCompressionStrategy(compressionStrategy)
            .setBatchGetLimit(2000)
            .setReadQuotaInCU(DEFAULT_PER_ROUTER_READ_QUOTA)
            .setChunkingEnabled(chunkingEnabled)
            .setIncrementalPushEnabled(incrementalPushEnabled);

    return createStoreForJob(veniceClusterName, keySchemaStr, valueSchemaStr, props, storeParams);
  }

  public static ControllerClient createStoreForJob(
      String veniceClusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      UpdateStoreQueryParams storeParams) {
    ControllerClient controllerClient = getControllerClient(veniceClusterName, props);
    NewStoreResponse newStoreResponse = controllerClient
        .createNewStore(props.getProperty(VENICE_STORE_NAME_PROP), "test@linkedin.com", keySchemaStr, valueSchemaStr);

    if (newStoreResponse.isError()) {
      throw new VeniceException("Could not create store " + props.getProperty(VENICE_STORE_NAME_PROP));
    }

    updateStore(veniceClusterName, props, storeParams.setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    return controllerClient;
  }

  public static void updateStore(String veniceClusterName, Properties props, UpdateStoreQueryParams params) {
    try (ControllerClient controllerClient = getControllerClient(veniceClusterName, props)) {
      ControllerResponse updateStoreResponse =
          controllerClient.retryableRequest(5, c -> c.updateStore(props.getProperty(VENICE_STORE_NAME_PROP), params));

      Assert.assertFalse(
          updateStoreResponse.isError(),
          "The UpdateStore response returned an error: " + updateStoreResponse.getError());
    }
  }

  private static ControllerClient getControllerClient(String veniceClusterName, Properties pushJobProps) {
    String veniceUrl = pushJobProps.getProperty(VENICE_DISCOVER_URL_PROP);
    String d2Prefix = "d2://";
    if (veniceUrl.startsWith(d2Prefix)) {
      final String d2ServiceName = veniceUrl.substring(d2Prefix.length());
      final String d2ZkHosts;
      boolean multiRegion = Boolean.parseBoolean(pushJobProps.get(MULTI_REGION).toString());
      if (multiRegion) {
        String parentControllerRegionName = pushJobProps.getProperty(PARENT_CONTROLLER_REGION_NAME);
        d2ZkHosts = pushJobProps.getProperty(D2_ZK_HOSTS_PREFIX + parentControllerRegionName);
      } else {
        String childControllerRegionName = pushJobProps.getProperty(SOURCE_GRID_FABRIC);
        d2ZkHosts = pushJobProps.getProperty(D2_ZK_HOSTS_PREFIX + childControllerRegionName);
      }
      return D2ControllerClientFactory
          .getControllerClient(d2ServiceName, veniceClusterName, d2ZkHosts, Optional.empty());
    } else {
      return ControllerClient.constructClusterControllerClient(veniceClusterName, veniceUrl);
    }
  }

  public static void sendStreamingRecordWithLogicalTimestamp(
      VeniceSystemProducer veniceProducer,
      String storeName,
      int index,
      long logicalTimestamp,
      boolean isDeleteOperation) {
    if (isDeleteOperation) {
      sendStreamingDeleteRecord(veniceProducer, storeName, Integer.toString(index), logicalTimestamp);
    } else {
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(index), "stream_" + index, logicalTimestamp);
    }
  }

  /**
   * Generate a streaming record using the provided producer to the specified store
   * key and value schema of the store must both be "string", the record produced is
   * based on the provided recordId
   */
  public static void sendStreamingRecord(SystemProducer producer, String storeName, int recordId) {
    sendStreamingRecord(producer, storeName, Integer.toString(recordId), "stream_" + recordId);
  }

  public static void sendStreamingRecordWithKeyPrefix(
      SystemProducer producer,
      String storeName,
      String keyPrefix,
      int recordId) {
    sendStreamingRecord(producer, storeName, keyPrefix + recordId, "stream_" + recordId);
  }

  public static void sendStreamingDeleteRecord(SystemProducer producer, String storeName, String key) {
    sendStreamingRecord(producer, storeName, key, null, null);
  }

  public static void sendStreamingDeleteRecord(
      SystemProducer producer,
      String storeName,
      String key,
      Long logicalTimeStamp) {
    sendStreamingRecord(producer, storeName, key, null, logicalTimeStamp);
  }

  public static void sendStreamingRecord(SystemProducer producer, String storeName, Object key, Object message) {
    sendStreamingRecord(producer, storeName, key, message, null);
  }

  public static void sendStreamingRecord(
      SystemProducer producer,
      String storeName,
      Object key,
      Object message,
      Long logicalTimeStamp) {
    OutgoingMessageEnvelope envelope;
    if (logicalTimeStamp == null) {
      envelope = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key, message);
    } else {
      envelope = new OutgoingMessageEnvelope(
          new SystemStream("venice", storeName),
          key,
          new VeniceObjectWithTimestamp(message, logicalTimeStamp));
    }
    producer.send(storeName, envelope);
    producer.flush(storeName);
  }

  /**
   * Identical to {@link #sendStreamingRecord(SystemProducer, String, int)} except that the value's length is equal
   * to {@param valueSizeInBytes}. The value is composed exclusively of the first digit of the {@param recordId}.
   *
   * @see #sendStreamingRecord(SystemProducer, String, int)
   */
  public static void sendCustomSizeStreamingRecord(
      SystemProducer producer,
      String storeName,
      int recordId,
      int valueSizeInBytes) {
    char[] chars = new char[valueSizeInBytes];
    Arrays.fill(chars, Integer.toString(recordId).charAt(0));
    sendStreamingRecord(producer, storeName, Integer.toString(recordId), new String(chars));
  }

  public static TopicManagerRepository getTopicManagerRepo(
      long kafkaOperationTimeoutMs,
      long topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      PubSubBrokerWrapper pubSubBrokerWrapper,
      PubSubTopicRepository pubSubTopicRepository) {
    Properties properties = new Properties();
    String pubSubBootstrapServers = pubSubBrokerWrapper.getAddress();
    properties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));
    properties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBootstrapServers);

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> new VeniceProperties(properties))
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubConsumerAdapterFactory(pubSubBrokerWrapper.getPubSubClientsFactory().getConsumerAdapterFactory())
            .setPubSubAdminAdapterFactory(pubSubBrokerWrapper.getPubSubClientsFactory().getAdminAdapterFactory())
            .setPubSubOperationTimeoutMs(kafkaOperationTimeoutMs)
            .setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollIntervalMs)
            .setTopicMinLogCompactionLagMs(topicMinLogCompactionLagMs)
            .build();

    return new TopicManagerRepository(topicManagerContext, pubSubBootstrapServers);
  }

  public static VeniceWriterFactory getVeniceWriterFactory(
      PubSubBrokerWrapper pubSubBrokerWrapper,
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    veniceWriterProperties
        .putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));
    return TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory);
  }

  public static void verifyConsumerThreadPoolFor(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper,
      String clusterName,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      ConsumerPoolType consumerPoolType,
      int expectedSourceRegionNumOnServer,
      int expectedReplicaNumPerRegion) {
    for (VeniceMultiClusterWrapper veniceMultiClusterWrapper: multiRegionMultiClusterWrapper.getChildRegions()) {
      int replicaPerRegionCount = 0;
      for (VeniceServerWrapper serverWrapper: veniceMultiClusterWrapper.getClusters()
          .get(clusterName)
          .getVeniceServers()) {
        KafkaStoreIngestionService kafkaStoreIngestionService =
            serverWrapper.getVeniceServer().getKafkaStoreIngestionService();
        ReplicaIngestionResponse replicaIngestionResponse =
            kafkaStoreIngestionService.getTopicPartitionIngestionContext(
                versionTopic.getName(),
                pubSubTopicPartition.getTopicName(),
                pubSubTopicPartition.getPartitionNumber());
        try {
          Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContexts =
              VENICE_JSON_SERIALIZER.deserialize(replicaIngestionResponse.getPayload(), "");
          if (!topicPartitionIngestionContexts.isEmpty()) {
            int regionCount = 0;
            for (Map.Entry<String, Map<String, TopicPartitionIngestionInfo>> entry: topicPartitionIngestionContexts
                .entrySet()) {
              Map<String, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap = entry.getValue();
              for (Map.Entry<String, TopicPartitionIngestionInfo> topicPartitionIngestionInfoEntry: topicPartitionIngestionInfoMap
                  .entrySet()) {
                String topicPartitionStr = topicPartitionIngestionInfoEntry.getKey();
                if (pubSubTopicPartition.toString().equals(topicPartitionStr)) {
                  TopicPartitionIngestionInfo topicPartitionIngestionInfo = topicPartitionIngestionInfoEntry.getValue();
                  assertTrue(topicPartitionIngestionInfo.getConsumerIdStr().contains(consumerPoolType.getStatSuffix()));
                  regionCount += 1;
                }
              }
            }
            // To ensure exactly one consumer from specific pool is allocated for each region.
            Assert.assertEquals(regionCount, expectedSourceRegionNumOnServer);
            replicaPerRegionCount += 1;
          }
        } catch (IOException e) {
          throw new VeniceException("Got IO Exception during consumer pool check.", e);
        }
      }
      Assert.assertEquals(replicaPerRegionCount, expectedReplicaNumPerRegion);
    }
  }
}
