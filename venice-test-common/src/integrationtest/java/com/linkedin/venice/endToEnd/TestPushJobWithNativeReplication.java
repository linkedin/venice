package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
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
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestPushJobWithNativeReplication {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 1 seconds;
     * Create a testing environment with 1 parent fabric and 1 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     * Override the KMM whitelist config so that only messages in admin topic will be replicaed by KMM.
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false,
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    childClusters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForBatchPush() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    int recordCount = 50;
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();
    /**
     * Enable L/F and native replication features.
     */
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(2)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childClusters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
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

  @Test(timeOut = TEST_TIMEOUT, groups = {"flaky"})
  public void testNativeReplicationWithLeadershipHandover() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    int recordCount = 10;
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(1)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);

    new Thread(() -> {
      KafkaPushJob job = new KafkaPushJob("Test push job", props);
      job.run();
    }).start();

    // Leadership hands over when the job is running
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, () -> {
      VeniceClusterWrapper veniceClusterWrapper = childClusters.get(NUMBER_OF_CHILD_DATACENTERS - 1).getClusters().get(clusterName);
      String topic = Version.composeKafkaTopic(storeName, 1);
      HelixBaseRoutingRepository routingDataRepo = veniceClusterWrapper.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepo.containsKafkaTopic(topic));

      Instance leaderNode = routingDataRepo.getLeaderInstance(topic, 0);
      Assert.assertNotNull(leaderNode);
      veniceClusterWrapper.stopAndRestartVeniceServer(leaderNode.getPort());
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childClusters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    });
  }
}
