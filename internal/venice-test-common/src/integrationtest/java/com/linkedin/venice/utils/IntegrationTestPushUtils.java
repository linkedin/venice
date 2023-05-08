package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.hadoop.VenicePushJob.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJob.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.producer.NearlineProducerFactory.JOB_ID;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.producer.NearlineProducerFactory.VENICE_STORE;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.NearlineProducer;
import com.linkedin.venice.producer.NearlineProducerFactory;
import com.linkedin.venice.producer.VeniceObjectWithTimestamp;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    props.putAll(KafkaSSLUtils.getLocalKafkaClientSSLConfig());
    return props;
  }

  /**
   * Blocking, waits for new version to go online
   */
  public static void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("hybrid-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          5,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
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

  public static Map<String, String> getNearlineProducerConfig(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type) {
    Map<String, String> nearlineProducerConfig = new HashMap<>();
    nearlineProducerConfig.put(VENICE_PUSH_TYPE, type.toString());
    nearlineProducerConfig.put(VENICE_STORE, storeName);
    nearlineProducerConfig.put(VENICE_CHILD_D2_ZK_HOSTS, venice.getZk().getAddress());
    nearlineProducerConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, VeniceControllerWrapper.D2_SERVICE_NAME);
    nearlineProducerConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_zk_address");
    nearlineProducerConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, "invalid_parent_d2_service");
    nearlineProducerConfig.put(JOB_ID, Utils.getUniqueString("venice-push-id"));
    nearlineProducerConfig.put(SSL_ENABLED, "false");
    return nearlineProducerConfig;
  }

  public static Map<String, String> getSystemProducerConfig(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type) {
    Map<String, String> nearlineProducerConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    nearlineProducerConfig.put(configPrefix + VENICE_PUSH_TYPE, type.toString());
    nearlineProducerConfig.put(configPrefix + VENICE_STORE, storeName);
    nearlineProducerConfig.put(configPrefix + VENICE_AGGREGATE, "false");
    nearlineProducerConfig.put(VENICE_CHILD_D2_ZK_HOSTS, venice.getZk().getAddress());
    nearlineProducerConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, VeniceControllerWrapper.D2_SERVICE_NAME);
    nearlineProducerConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_zk_address");
    nearlineProducerConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, "invalid_parent_d2_service");
    nearlineProducerConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    nearlineProducerConfig.put(SSL_ENABLED, "false");
    return nearlineProducerConfig;
  }

  @SafeVarargs
  public static NearlineProducer getNearlineProducer(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type,
      Pair<String, String>... optionalConfigs) {
    Map<String, String> nearlineProducerConfig = getNearlineProducerConfig(venice, storeName, type);
    for (Pair<String, String> config: optionalConfigs) {
      nearlineProducerConfig.put(config.getFirst(), config.getSecond());
    }
    Properties nearlineProducerProps = new Properties();
    nearlineProducerProps.putAll(nearlineProducerConfig);
    NearlineProducer veniceProducer =
        NearlineProducerFactory.getInstance().getProducer(new VeniceProperties(nearlineProducerProps), null);
    veniceProducer.start();
    return veniceProducer;
  }

  @SafeVarargs
  public static VeniceSystemProducer getSystemProducer(
      VeniceClusterWrapper venice,
      String storeName,
      Version.PushType type,
      Pair<String, String>... optionalConfigs) {
    Map<String, String> systemProducerConfig = getSystemProducerConfig(venice, storeName, type);
    for (Pair<String, String> config: optionalConfigs) {
      systemProducerConfig.put(config.getFirst(), config.getSecond());
    }
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(systemProducerConfig), null);
    veniceProducer.start();
    return (VeniceSystemProducer) veniceProducer;
  }

  public static PubSubConsumerAdapterFactory getVeniceConsumerFactory() {
    return new ApacheKafkaConsumerAdapterFactory();
  }

  public static PubSubAdminAdapterFactory getVeniceAdminFactory() {
    return new ApacheKafkaAdminAdapterFactory();
  }

  public static ControllerClient createStoreForJob(String veniceClusterName, Schema recordSchema, Properties props) {
    return createStoreForJob(
        veniceClusterName,
        recordSchema.getField(props.getProperty(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP)).schema().toString(),
        recordSchema.getField(props.getProperty(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP)).schema().toString(),
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

    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

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

  /**
   * Generate a streaming record using the provided producer to the specified store
   * key and value schema of the store must both be "string", the record produced is
   * based on the provided recordId
   */
  public static void sendStreamingRecord(NearlineProducer producer, int recordId) {
    sendStreamingRecord(producer, Integer.toString(recordId), "stream_" + recordId);
  }

  public static void sendStreamingRecordWithKeyPrefix(NearlineProducer producer, String keyPrefix, int recordId) {
    sendStreamingRecord(producer, keyPrefix + recordId, "stream_" + recordId);
  }

  public static void sendStreamingDeleteRecord(NearlineProducer producer, String key) {
    sendStreamingRecord(producer, key, null, null);
  }

  public static void sendStreamingDeleteRecord(NearlineProducer producer, String key, Long logicalTimeStamp) {
    sendStreamingRecord(producer, key, null, logicalTimeStamp);
  }

  public static void sendStreamingRecord(NearlineProducer producer, Object key, Object message) {
    sendStreamingRecord(producer, key, message, null);
  }

  public static void sendStreamingRecord(NearlineProducer producer, Object key, Object message, Long logicalTimeStamp) {
    producer.send(key, logicalTimeStamp == null ? message : new VeniceObjectWithTimestamp(message, logicalTimeStamp));
    producer.flush();
  }

  public static void sendStreamingRecord(VeniceSystemProducer producer, String storeName, Object key, Object message) {
    sendStreamingRecord(producer, storeName, key, message, null);
  }

  public static void sendStreamingRecord(
      VeniceSystemProducer producer,
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
  public static void sendCustomSizeStreamingRecord(NearlineProducer producer, int recordId, int valueSizeInBytes) {
    char[] chars = new char[valueSizeInBytes];
    Arrays.fill(chars, Integer.toString(recordId).charAt(0));
    sendStreamingRecord(producer, Integer.toString(recordId), new String(chars));
  }

  public static TopicManagerRepository getTopicManagerRepo(
      long kafkaOperationTimeoutMs,
      long topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      String pubSubBootstrapServers,
      PubSubTopicRepository pubSubTopicRepository) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBootstrapServers);
    return TopicManagerRepository.builder()
        .setPubSubProperties(k -> new VeniceProperties(properties))
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setLocalKafkaBootstrapServers(pubSubBootstrapServers)
        .setPubSubConsumerAdapterFactory(getVeniceConsumerFactory())
        .setPubSubAdminAdapterFactory(getVeniceAdminFactory())
        .setKafkaOperationTimeoutMs(kafkaOperationTimeoutMs)
        .setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollIntervalMs)
        .setTopicMinLogCompactionLagMs(topicMinLogCompactionLagMs)
        .build();
  }
}
