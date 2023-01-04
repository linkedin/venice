package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import com.linkedin.venice.samza.VeniceSystemFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;


public class IntegrationTestPushUtils {
  public static Properties defaultVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    return TestPushUtils
        .defaultVPJProps(veniceCluster.getRandomVeniceController().getControllerUrl(), inputDirPath, storeName);
  }

  public static Properties sslVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.putAll(KafkaSSLUtils.getLocalKafkaClientSSLConfig());
    return props;
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

  public static void makeStoreLF(VeniceClusterWrapper venice, String storeName) {
    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(venice.getClusterName(), venice.getRandomRouterURL())) {
      ControllerResponse response =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    }
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

  public static KafkaClientFactory getVeniceConsumerFactory(KafkaBrokerWrapper kafka) {
    return new TestKafkaClientFactory(kafka.getAddress(), kafka.getZkAddress());
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

  private static ControllerClient getControllerClient(String veniceClusterName, Properties props) {
    String veniceUrl = props.getProperty(VENICE_DISCOVER_URL_PROP);
    return ControllerClient.constructClusterControllerClient(veniceClusterName, veniceUrl);
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

  private static class TestKafkaClientFactory extends KafkaClientFactory {
    private final String kafkaBootstrapServers;
    private final String kafkaZkAddress;

    public TestKafkaClientFactory(String kafkaBootstrapServers, String kafkaZkAddress) {
      this.kafkaBootstrapServers = kafkaBootstrapServers;
      this.kafkaZkAddress = kafkaZkAddress;
    }

    @Override
    public Properties setupSSL(Properties properties) {
      properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
      return properties;
    }

    @Override
    protected String getKafkaAdminClass() {
      return KafkaAdminClient.class.getName();
    }

    @Override
    protected String getWriteOnlyAdminClass() {
      return getKafkaAdminClass();
    }

    @Override
    protected String getReadOnlyAdminClass() {
      return getKafkaAdminClass();
    }

    @Override
    protected String getKafkaZkAddress() {
      return kafkaZkAddress;
    }

    @Override
    public String getKafkaBootstrapServers() {
      return kafkaBootstrapServers;
    }

    @Override
    protected KafkaClientFactory clone(
        String kafkaBootstrapServers,
        String kafkaZkAddress,
        Optional<MetricsParameters> metricsParameters) {
      return new TestKafkaClientFactory(kafkaBootstrapServers, kafkaZkAddress);
    }
  }
}
