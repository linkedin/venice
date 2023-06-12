package com.linkedin.venice.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestWriteUtils.writeSchemaWithUnknownFieldIntoAvroFile;

import com.google.common.collect.ImmutableMap;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
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
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class EndToEndKafkaWithSASLTest {
  ZkServerWrapper zkServer;
  PubSubBrokerWrapper pubSubBrokerWrapper;
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    Utils.thisIsLocalhost();
    zkServer = ServiceFactory.getZkServer();
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer)
            .setAdditionalBrokerConfiguration(
                ImmutableMap.of(
                    "listeners",
                    "SASL_PLAINTEXT://$HOSTNAME:$PORT",
                    "inter.broker.listener.name",
                    "SASL_PLAINTEXT",
                    "sasl.mechanism.inter.broker.protocol",
                    "PLAIN",
                    "sasl.enabled.mechanisms",
                    "PLAIN",
                    "listener.name.sasl_plaintext.plain.sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                        + "    user_admin=\"admin-secret\" username=\"admin\" password=\"admin-secret\"\n" + ";"))
            .build());
    Properties extraProperties = new Properties();
    extraProperties.put("kafka.security.protocol", "SASL_PLAINTEXT");
    extraProperties.put("kafka.sasl.mechanism", "PLAIN");
    extraProperties.put(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"admin\" password=\"admin-secret\";");
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(1)
        .minActiveReplica(0)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .zkServerWrapper(zkServer)
        .kafkaBrokerWrapper(pubSubBrokerWrapper)
        .extraProperties(extraProperties)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test
  public void testPushJob() throws Exception {
    File inputDir = getTempDataDirectory();

    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSchemaWithUnknownFieldIntoAvroFile(inputDir));
    String storeName = Utils.getUniqueString("store");

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    props.put("kafka.security.protocol", "SASL_PLAINTEXT");
    props.put("kafka.sasl.mechanism", "PLAIN");
    props.put(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required " + "    username=\"admin\" "
            + "    password=\"admin-secret\";");

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
            .setMetricsRepository(metricsRepository)); // metrics only available for Avro client...
    ) {
      String schemaWithoutSymbolDocStr = loadFileAsString("SchemaWithoutSymbolDoc.avsc");
      Schema schemaWithoutSymbolDoc = AvroCompatibilityHelper.parse(schemaWithoutSymbolDocStr);
      GenericRecord keyRecord =
          new GenericData.Record(schemaWithoutSymbolDoc.getField(DEFAULT_KEY_FIELD_PROP).schema());
      Schema sourceSchema = keyRecord.getSchema().getField("source").schema();
      keyRecord.put("memberId", (long) 1);
      keyRecord.put(
          "source",
          AvroCompatibilityHelper.newEnumSymbol(sourceSchema, TestWriteUtils.TestRecordType.OFFLINE.toString()));
      IndexedRecord value = (IndexedRecord) avroClient.get(keyRecord).get();
      Assert.assertEquals(value.get(0).toString(), "LOGO");
      Assert.assertEquals(value.get(1), 1);
    }
  }
}
