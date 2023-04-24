package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminTool {
  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;
  private final String testStoreName = Utils.getUniqueString();
  private String clusterName;
  private ControllerClient parentControllerClient;
  private ControllerClient childControllerClient;
  private String testKey = "test_key";
  private String testValue = "test_value";

  private String childControllerUrl;
  private String childKafkaAddress;
  private String parentKafkaAddress;
  private String testStoreVersionTopicName;

  /**
   * There are a couple of reasons to have an integration test here:
   * 1. Code coverage doesn't count integration test.
   * 2. It is hard to write a meaningful mock test against AdminTool.
   */
  @BeforeClass
  public void setUp() {
    // Disable system store auto-materialization
    Properties controllerProps = new Properties();
    controllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    controllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");

    twoLayerMultiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        1,
        1,
        1,
        2,
        0,
        1,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.empty());

    clusterName = twoLayerMultiRegionMultiClusterWrapper.getClusterNames()[0];
    // To make sure Parent Controller is ready
    twoLayerMultiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    parentKafkaAddress = twoLayerMultiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper().getAddress();
    // initialize parent Controller Client
    parentControllerClient =
        new ControllerClient(clusterName, twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString());
    // initialize child Controller Client
    VeniceMultiClusterWrapper multiCluster = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    childControllerUrl = multiCluster.getControllerConnectString();
    childKafkaAddress = multiCluster.getKafkaBrokerWrapper().getAddress();
    childControllerClient = new ControllerClient(clusterName, childControllerUrl);

    String schema = "\"string\"";

    // Create store
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(testStoreName, "tester", schema, schema);
    Assert.assertFalse(newStoreResponse.isError(), "Store creation failed with error: " + newStoreResponse.getError());
    // Make sure the store exists in Child fabric
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(testStoreName);
      Assert.assertFalse(storeResponse.isError(), "Store lookup failed with error: " + storeResponse.getError());
      Assert.assertTrue(storeResponse.getStore() != null, "Store response shouldn't be null");
    });

    // Create a new version and write some data
    VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
        testStoreName,
        10000,
        Version.PushType.BATCH,
        "test_push_id",
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    Assert.assertFalse(
        versionCreationResponse.isError(),
        "Failed to create a new version, error: " + versionCreationResponse.getError());
    testStoreVersionTopicName = versionCreationResponse.getKafkaTopic();
    // Write some data to the new version
    Properties props = new Properties();
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, versionCreationResponse.getKafkaBootstrapServers());
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(props);
    VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(testStoreVersionTopicName)
            .setKeySerializer(new VeniceAvroKafkaSerializer(schema))
            .setValueSerializer(new VeniceAvroKafkaSerializer(schema))
            .build());
    veniceWriter.put(testKey, testValue, 1);
    veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    veniceWriter.flush();
    veniceWriter.close();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(testStoreName);
      Assert.assertFalse(storeResponse.isError(), "Store lookup failed with error: " + storeResponse.getError());
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
    });

  }

  @AfterClass
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(childControllerClient);
  }

  @Test
  public void testDumpKafkaTopic() throws Exception {
    String[] args = { "--dump-kafka-topic", "--kafka-bootstrap-servers", childKafkaAddress, // Version topic only exists
        // in child Kafka clsuter
        "--kafka-topic-name", testStoreVersionTopicName, "--kafka-topic-partition", "0", "--url", childControllerUrl,
        "--cluster", clusterName, "--log-metedata", "true" };
    AdminTool.main(args);
  }

  @Test
  public void testDumpAdminMessages() throws Exception {
    String[] args = { "--dump-admin-messages", "--kafka-bootstrap-servers", parentKafkaAddress, // Admin topic only
        // exists in parent
        // Kafka cluster
        "--kafka-topic-name", AdminTopicUtils.getTopicNameFromClusterName(clusterName), "--starting_offset", "0",
        "--message_count", "100", "--url", childControllerUrl, "--cluster", clusterName };
    AdminTool.main(args);
  }

  @Test
  public void testQueryKafkaTopic() throws Exception {
    String[] args = { "--query-kafka-topic", "--kafka-bootstrap-servers", childKafkaAddress, "--kafka-topic-name",
        testStoreVersionTopicName, "--start-date", "2023-04-20 00:00:00", "--end-date", "2030-04-20 00:00:00", "--key",
        testKey, "--progress-interval", "100", "--url", childControllerUrl, "--cluster", clusterName };
    AdminTool.main(args);
  }

}
