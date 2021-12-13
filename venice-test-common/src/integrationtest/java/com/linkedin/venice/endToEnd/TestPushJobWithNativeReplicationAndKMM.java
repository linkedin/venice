package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestPushJobWithNativeReplicationAndKMM {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][]{{50, 2}};
  }


  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     * KMM whitelist config allows replicating all topics.
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
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
        false,
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    //setup an additional MirrorMaker between dc-0 and dc-1
    multiColoMultiClusterWrapper.addMirrorMakerBetween(childDatacenters.get(0), childDatacenters.get(1), MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiColoMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();
    /**
     * Enable L/F and native replication features.
     */
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(partitionCount);

    ControllerClient parentControllerClient = createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);
    /**
     * Only enable L/F and NR in parent controllers and the child controllers of dc-2 fabric
     */
    UpdateStoreQueryParams enableLFAndNR = new UpdateStoreQueryParams()
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true)
        .setRegionsFilter("dc-2,parent.parent");
    ControllerResponse updateStoreResponse = parentControllerClient.updateStore(storeName, enableLFAndNR);
    Assert.assertFalse(updateStoreResponse.isError());
    parentControllerClient.close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName,
            childDatacenters.get(2).getControllerConnectString())) {

      //verify all the datacenter is configured correctly.
      verifyDCConfigNativeRepl(dc0Client, storeName, false);
      verifyDCConfigNativeRepl(dc1Client, storeName, false);
      verifyDCConfigNativeRepl(dc2Client, storeName, true);
    }

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();

      //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    });
  }

  public static void verifyDCConfigNativeRepl(ControllerClient controllerClient, String storeName, boolean enabled) {
    verifyDCConfigNativeRepl(controllerClient, storeName, enabled, Optional.empty());
  }

  public static void verifyDCConfigNativeRepl(ControllerClient controllerClient, String storeName, boolean enabled, Optional<String> nativeReplicationSource) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      Assert.assertEquals(storeResponse.getStore().isNativeReplicationEnabled(), enabled, "The native replication config does not match.");
      nativeReplicationSource.ifPresent(s -> Assert.assertEquals(storeResponse.getStore().getNativeReplicationSourceFabric(), s, "Native replication source doesn't match"));
    });
  }
}
