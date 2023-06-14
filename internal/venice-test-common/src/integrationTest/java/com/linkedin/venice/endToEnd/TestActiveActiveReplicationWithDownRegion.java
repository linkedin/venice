package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.utils.TestWriteUtils.INT_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class TestActiveActiveReplicationWithDownRegion {
  private static final Logger LOGGER = LogManager.getLogger(TestActiveActiveReplicationWithDownRegion.class);

  private static final int TEST_TIMEOUT = 90_000; // ms
  private static final int RECORDS_TO_POPULATE = 4;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  // ["venice-cluster0", "venice-cluster1", ...];

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  public Map<String, String> getExtraServerProperties() {
    return Collections.emptyMap();
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 second;
     * Create a testing environment with 1 parent fabric and 2 child fabrics (one where the broker will be healthy (our source fabric)
     * and another where the broker is having a problem); Set server and replication factor to 2 to ensure at least 1 leader
     * replica and 1 follower replica;
     */
    Properties serverProperties = new Properties();
    // We're going to trigger timeouts. Set this lower to improve developer happiness
    serverProperties.put(KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC, 10L);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");
    serverProperties.put(SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS, 5);
    getExtraServerProperties().forEach(serverProperties::put);

    Properties controllerProps = new Properties();
    controllerProps.put(KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC, 10L);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(controllerProps),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    // TODO: This takes FOREVER when we close a kafka broker prematurely, BUT it does finish... There seems to be a
    // problem
    // with how we handle processes that are closed are already.
    // ApacheKafkaAdminAdapter that bemoans it's lost broker for a long time before timing out and giving up (I think in
    // the
    // controller).
    multiRegionMultiClusterWrapper.close();
  }

  // TODO: This needs some work. It's very slow, and currently hangs on cleanup. We need to refactor how the cluster
  // wrappers handle
  // the cleanup as well as think of ways to speed this up. Currently there are a sprinkling of kafka retries and
  // timeouts
  // that are hardcoded. Ideally we'd have these fully configurable to make this test finish in a predictable and
  // reasonable amount of time
  // @Test(timeOut = TEST_TIMEOUT)
  public void testDownedKafka() throws Exception {
    // These variable don't do anything other than to make it easy to find their values in the debugger so you can hook
    // up ZooInspector and figure which region is assigned where
    int zkPort = multiRegionMultiClusterWrapper.getZkServerWrapper().getPort();
    int dc0Kafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getKafkaBrokerWrapper().getPort();
    int dc1kafka = multiRegionMultiClusterWrapper.getChildRegions().get(1).getKafkaBrokerWrapper().getPort();

    // Spotbug doesn't like unused variables. Given they are assigned for debugging purposes, print them out.
    LOGGER.info("zkPort: {}", zkPort);
    LOGGER.info("dc0Kafka: {}", dc0Kafka);
    LOGGER.info("dc1kafka: {}", dc1kafka);

    // Create a store in all regions with A/A and hybrid enabled
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test-store");
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      parentControllerClient.createNewStore(storeName, "owner", INT_SCHEMA, STRING_SCHEMA);
      TestUtils.updateStoreToHybrid(
          storeName,
          parentControllerClient,
          Optional.of(true),
          Optional.of(true),
          Optional.of(false));

      // Empty push to create a version
      parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
    }

    // Verify that version 1 is created in all regions
    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS; i++) {
      try (ControllerClient childControllerClient = new ControllerClient(
          clusterName,
          childDatacenters.get(i).getLeaderController(clusterName).getControllerUrl())) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        });
      }
    }

    // A store has been created with version 1 in all regions that is hybrid and A/A
    // Now lets populate some data into dc-0 and verify that records replicate to all regions
    // Build a system producer that writes nearline to dc-0
    SystemProducer producerInDC0 = new VeniceSystemProducer(
        childDatacenters.get(0).getZkServerWrapper().getAddress(),
        childDatacenters.get(0).getZkServerWrapper().getAddress(),
        D2_SERVICE_NAME,
        storeName,
        Version.PushType.STREAM,
        Utils.getUniqueString("venice-push-id"),
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty());
    producerInDC0.start();

    SystemProducer producerInDC1 = new VeniceSystemProducer(
        childDatacenters.get(1).getZkServerWrapper().getAddress(),
        childDatacenters.get(1).getZkServerWrapper().getAddress(),
        D2_SERVICE_NAME,
        storeName,
        Version.PushType.STREAM,
        Utils.getUniqueString("venice-push-id"),
        "dc-1",
        true,
        null,
        Optional.empty(),
        Optional.empty());
    producerInDC1.start();

    // Build another one which will write some batch data
    SystemProducer batchProducer = new VeniceSystemProducer(
        childDatacenters.get(0).getZkServerWrapper().getAddress(),
        multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress(),
        PARENT_D2_SERVICE_NAME,
        storeName,
        Version.PushType.BATCH,
        Utils.getUniqueString("venice-push-id"),
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty());
    batchProducer.start();

    // Send a few keys, and close out the system writer
    for (int rowIncrement = 0; rowIncrement < RECORDS_TO_POPULATE; rowIncrement++) {
      String value1 = "value" + rowIncrement;
      OutgoingMessageEnvelope envelope1 =
          new OutgoingMessageEnvelope(new SystemStream("venice", storeName), rowIncrement, value1);
      producerInDC0.send(storeName, envelope1);
    }
    producerInDC0.stop();

    // Send a few keys, and close out the system writer
    for (int rowIncrement = 0; rowIncrement < RECORDS_TO_POPULATE; rowIncrement++) {
      String value1 = "value1" + rowIncrement;
      OutgoingMessageEnvelope envelope1 =
          new OutgoingMessageEnvelope(new SystemStream("venice", storeName), rowIncrement + 10, value1);
      producerInDC1.send(storeName, envelope1);
    }
    producerInDC1.stop();

    // Validate keys have been written to all regions
    for (String cluster: CLUSTER_NAMES) {
      String routerUrl = childDatacenters.get(0).getClusters().get(cluster).getRandomRouterURL();
      try (AvroGenericStoreClient<Integer, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        // TODO: It seems to take an awfully long time for the hybrid data to percolate in this test setup. Be nice to
        // puzzle out why.
        TestUtils.waitForNonDeterministicAssertion(80, TimeUnit.SECONDS, () -> {
          for (int rowIncrement = 0; rowIncrement < RECORDS_TO_POPULATE; rowIncrement++) {
            Object valueObject = client.get(rowIncrement).get();
            Assert.assertNotNull(valueObject, "Cluster:" + cluster + " didn't have key:" + rowIncrement);
            Assert.assertEquals(valueObject.toString(), "value" + rowIncrement);
          }
        });
      }
    }

    // TODO: Consider moving all of the above into the 'setUp' function as it's laying the ground work for all tests in
    // this
    // suite that might expect a downed Kafka broker

    // Ok. So if we've gotten this far, everything is working. Neat. Now lets change that. We're going to kill the kafka
    // broker in one of the regions, and then we're going to execute a new push. Here is what should happen. The new
    // push
    // should succeed in the OTHER regions and go live.

    // It's simple, we kill the kafka broker
    multiRegionMultiClusterWrapper.getChildRegions()
        .get(NUMBER_OF_CHILD_DATACENTERS - 1)
        .getKafkaBrokerWrapper()
        .close();

    // Execute a new push by writing some rows and sending an endOfPushMessage
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      for (int rowIncrement = 0; rowIncrement < RECORDS_TO_POPULATE; rowIncrement++) {
        String value1 = "value" + rowIncrement;
        OutgoingMessageEnvelope envelope1 =
            new OutgoingMessageEnvelope(new SystemStream("venice", storeName), rowIncrement, value1);
        batchProducer.send(storeName, envelope1);
      }
      // close out the push
      parentControllerClient.writeEndOfPush(storeName, 2);
    }

    // Let's verify from the other two regions
    for (int i = 0; i < NUMBER_OF_CHILD_DATACENTERS - 1; i++) {
      try (ControllerClient childControllerClient = new ControllerClient(
          clusterName,
          childDatacenters.get(i).getLeaderController(clusterName).getControllerUrl())) {
        TestUtils.waitForNonDeterministicAssertion(6000, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = childControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError());
          // We should have at least two versions in flight (serving and backup, but hopefully not future!)
          // Assert.assertEquals(storeResponse.getStore().getVersions().size(), 2);
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
        });
      }
    }
  }
}
