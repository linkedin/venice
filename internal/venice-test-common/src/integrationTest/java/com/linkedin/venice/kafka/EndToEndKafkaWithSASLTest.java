package com.linkedin.venice.kafka;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSchemaWithUnknownFieldIntoAvroFile;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class EndToEndKafkaWithSASLTest {
  private ZkServerWrapper zkServer;
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private VeniceClusterWrapper veniceCluster;

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobWithSaslSsl() throws Exception {
    runTestPushJob(PubSubSecurityProtocol.SASL_SSL);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobWithSaslPlaintext() throws Exception {
    runTestPushJob(PubSubSecurityProtocol.SASL_PLAINTEXT);
  }

  private void runTestPushJob(PubSubSecurityProtocol securityProtocol) throws Exception {
    Utils.thisIsLocalhost();
    zkServer = ServiceFactory.getZkServer();

    Properties extraProperties = new Properties();
    Map<String, String> additionalBrokerConfigs = new HashMap<>();
    additionalBrokerConfigs.put("inter.broker.listener.name", securityProtocol.name());
    additionalBrokerConfigs.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
    additionalBrokerConfigs.put("sasl.enabled.mechanisms", "PLAIN");

    String listenerName = securityProtocol.name().toLowerCase();
    String protocolConfig = "listener.name." + listenerName + ".plain.sasl.jaas.config";
    additionalBrokerConfigs.put(
        protocolConfig,
        "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
            + "    user_admin=\"admin-secret\" username=\"admin\" password=\"admin-secret\";\n");

    if (securityProtocol == PubSubSecurityProtocol.SASL_SSL) {
      additionalBrokerConfigs.put("listeners", "SASL_SSL://$HOSTNAME:$PORT");
      SslUtils.VeniceTlsConfiguration tlsConfig = SslUtils.getTlsConfiguration();
      extraProperties.put("kafka.security.protocol", "SASL_SSL");
      extraProperties.put("kafka." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, tlsConfig.getKeyStorePath());
      extraProperties.put("kafka." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, tlsConfig.getKeyStorePassword());
      extraProperties.put("kafka." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, tlsConfig.getKeyStoreType());
      extraProperties.put("kafka." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, tlsConfig.getKeyPassphrase());
      extraProperties.put("kafka." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tlsConfig.getTrustStorePath());
      extraProperties.put("kafka." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tlsConfig.getTrustStorePassword());
      extraProperties.put("kafka." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, tlsConfig.getTrustStoreType());
      extraProperties.put("kafka." + SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, tlsConfig.getKeyManagerAlgorithm());
      extraProperties
          .put("kafka." + SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, tlsConfig.getTrustManagerAlgorithm());
      extraProperties
          .put("kafka." + SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, tlsConfig.getSecureRandomAlgorithm());
    } else {
      additionalBrokerConfigs.put("listeners", "SASL_PLAINTEXT://$HOSTNAME:$PORT");
      extraProperties.put("kafka.security.protocol", "SASL_PLAINTEXT");
    }

    extraProperties.put("kafka.sasl.mechanism", "PLAIN");
    extraProperties.put(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"admin\" password=\"admin-secret\";");

    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer)
            .setRegionName(STANDALONE_REGION_NAME)
            .setAdditionalBrokerConfiguration(additionalBrokerConfigs)
            .build());

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(1)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .zkServerWrapper(zkServer)
        .kafkaBrokerWrapper(pubSubBrokerWrapper)
        .extraProperties(extraProperties)
        .build();

    veniceCluster = ServiceFactory.getVeniceCluster(options);

    // ---- Begin test logic ----
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSchemaWithUnknownFieldIntoAvroFile(inputDir));
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    props.putAll(extraProperties); // include security settings for client side

    createStoreForJob(
        veniceCluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    TestWriteUtils.runPushJob("Test Batch push job", props);
    veniceCluster.refreshAllRouterMetaData();

    MetricsRepository metricsRepository = new MetricsRepository();
    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      String schemaStr = loadFileAsString("SchemaWithoutSymbolDoc.avsc");
      Schema schema = AvroCompatibilityHelper.parse(schemaStr);
      GenericRecord keyRecord = new GenericData.Record(schema.getField(DEFAULT_KEY_FIELD_PROP).schema());
      Schema sourceSchema = keyRecord.getSchema().getField("source").schema();
      keyRecord.put("memberId", 1L);
      keyRecord.put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, "OFFLINE"));
      IndexedRecord value = (IndexedRecord) avroClient.get(keyRecord).get();
      Assert.assertEquals(value.get(0).toString(), "LOGO");
      Assert.assertEquals(value.get(1), 1);
    }
  }
}
