package com.linkedin.venice;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * This class initializes a cluster and pushes some synthetic data into a store.
 */
public class VeniceClusterInitializer implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(VeniceClusterInitializer.class);

  public static final String VALUE_SCHEMA_STR = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\", \"default\": \"default_id\"},             "
      + "         { \"name\": \"name\", \"type\": \"string\", \"default\": \"default_name\"},           "
      + "         { \"name\": \"boolean_field\", \"type\": \"boolean\", \"default\": false},           "
      + "         { \"name\": \"int_field\", \"type\": \"int\", \"default\": 0},           "
      + "         { \"name\": \"float_field\", \"type\": \"float\", \"default\": 0.0},           "
      + "         { \"name\": \"namemap\", \"type\":  {\"type\" : \"map\", \"values\" : \"int\" }},           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" }, \"default\": []},"
      + "         { \"name\": \"ZookeeperAddress\", \"type\": \"string\"}" + "  ]       " + " }       ";
  public static final String KEY_SCHEMA_STR = "\"string\"";
  public static final String KEY_PREFIX = "key_";
  public static final String ID_FIELD_PREFIX = "id_";
  public static final String ZK_ADDRESS_FIELD = "ZookeeperAddress";
  public static final int ENTRY_COUNT = 1000;

  private final VeniceClusterWrapper veniceCluster;
  private final AvroGenericStoreClient<String, Object> regularStoreClient;
  private final int valueSchemaId;
  private final String pushVersionTopic;
  private final int pushVersion;
  private final ControllerClient controllerClient;

  private final String storeName;
  private final VeniceKafkaSerializer keySerializer;
  private final VeniceKafkaSerializer valueSerializer;

  public VeniceClusterInitializer(String storeName, int routerPort) {
    Properties clusterConfig = new Properties();
    clusterConfig.put(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    clusterConfig.put(ConfigKeys.ROUTER_ENABLE_SSL, false);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(0)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(clusterConfig)
        .build();
    this.veniceCluster = ServiceFactory.getVeniceCluster(options);
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_COMPUTE_FAST_AVRO_ENABLED, true);
    this.veniceCluster.addVeniceServer(new Properties(), serverProperties);

    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    routerProperties.put(ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, false);
    routerProperties.put(ROUTER_PORT_TO_USE_IN_VENICE_ROUTER_WRAPPER, Integer.toString(routerPort));
    this.veniceCluster.addVeniceRouter(routerProperties);
    String routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();
    LOGGER.info("Router address: {}", routerAddr);

    this.storeName = storeName;
    // Create test store
    this.controllerClient = this.veniceCluster.getControllerClient();
    TestUtils.assertCommand(controllerClient.createNewStore(storeName, "test_owner", KEY_SCHEMA_STR, VALUE_SCHEMA_STR));
    this.veniceCluster.createMetaSystemStore(storeName);
    // Enable read compute
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
    VersionCreationResponse newVersion = TestUtils.assertCommand(
        controllerClient.requestTopicForWrites(
            storeName,
            10240000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1));
    this.pushVersion = newVersion.getVersion();
    this.pushVersionTopic = newVersion.getKafkaTopic();
    this.valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    this.keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    this.valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    this.regularStoreClient = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr));
    try {
      pushSyntheticData();
    } catch (Exception e) {
      throw new VeniceException("Failed to push synthetic data", e);
    }
    LOGGER.info("Finished pushing the synthetic data");

    // Quick verification
    try {
      quickVerification();
    } catch (Exception e) {
      throw new VeniceException("Failed to validate the push", e);
    }
    LOGGER.info("Finished quick verification");
  }

  private void quickVerification() throws ExecutionException, InterruptedException {
    // Issue a single-get request
    String key0 = KEY_PREFIX + "0";
    Object value = regularStoreClient.get(key0).get();
    if (value == null) {
      throw new VeniceException("Failed to retrieve value for key: " + key0);
    }
    if (!(value instanceof GenericRecord)) {
      throw new VeniceException("The returned value should be a GenericRecord");
    }

    GenericRecord genericRecord = (GenericRecord) value;
    String id = genericRecord.get("id").toString();
    String expectedId = ID_FIELD_PREFIX + "0";
    if (!id.equals(expectedId)) {
      throw new VeniceException("Expected the value for 'id' field: " + expectedId + ", but got: " + id);
    }
  }

  private void pushSyntheticData() throws ExecutionException, InterruptedException {
    Schema valueSchema = Schema.parse(VALUE_SCHEMA_STR);
    // Insert test record
    List<byte[]> values = new ArrayList<>(ENTRY_COUNT);
    for (int i = 0; i < ENTRY_COUNT; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", ID_FIELD_PREFIX + i);
      String name = "name_" + i;
      value.put("name", name);
      value.put("namemap", Collections.emptyMap());
      value.put("boolean_field", true);
      value.put("int_field", 10);
      value.put("float_field", 10.0f);
      value.put(ZK_ADDRESS_FIELD, veniceCluster.getZk().getAddress());

      List<Float> features = new ArrayList<>();
      features.add(Float.valueOf((float) (i + 1)));
      features.add(Float.valueOf((float) ((i + 1) * 10)));
      value.put("member_feature", features);
      byte[] serializedBytes = valueSerializer.serialize(pushVersionTopic, value);
      values.add(i, serializedBytes);
    }

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    try (VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(pushVersionTopic).setKeyPayloadSerializer(keySerializer).build())) {
      veniceWriter.broadcastStartOfPush(Collections.emptyMap());
      Future[] writerFutures = new Future[ENTRY_COUNT];
      for (int i = 0; i < ENTRY_COUNT; i++) {
        writerFutures[i] = veniceWriter.put(KEY_PREFIX + i, values.get(i), valueSchemaId);
      }

      // wait synchronously for them to succeed
      for (int i = 0; i < ENTRY_COUNT; ++i) {
        writerFutures[i].get();
      }
      // Write end of push message to make node become ONLINE from BOOTSTRAP
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      String status = controllerClient.queryJobStatus(pushVersionTopic).getStatus();
      if (ExecutionStatus.isError(status)) {
        // Not recoverable (at least not without re-pushing), so not worth spinning our wheels until the timeout.
        throw new VeniceException("Push failed.");
      }

      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      // Refresh router metadata once new version is pushed, so that the router sees the latest store version.
      if (currentVersion == pushVersion) {
        veniceCluster.refreshAllRouterMetaData();
      }
      Assert.assertEquals(currentVersion, pushVersion, "New version not online yet.");
    });
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(regularStoreClient);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  /**
   * This main function is expecting two arguments:
   * 1. store name.
   * 2. router port.
   * @param args
   */
  public static void main(String[] args) {
    LOGGER.info("Avro version in VeniceClusterInitializer: {}", AvroCompatibilityHelperCommon.getRuntimeAvroVersion());
    Assert.assertEquals(AvroCompatibilityHelperCommon.getRuntimeAvroVersion(), AvroVersion.AVRO_1_10);
    Assert.assertEquals(args.length, 2, "Store name and router port arguments are expected");

    String storeName = args[0];
    int routerPort = Integer.parseInt(args[1]);
    VeniceClusterInitializer clusterInitializer = new VeniceClusterInitializer(storeName, routerPort);

    Runtime.getRuntime().addShutdownHook(new Thread(clusterInitializer::close));
  }
}
