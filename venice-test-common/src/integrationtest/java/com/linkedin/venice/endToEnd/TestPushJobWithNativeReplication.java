package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestPushJobWithNativeReplication {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 1 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     * Override the KMM whitelist config so that only messages in admin topic will be replicaed by KMM.
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 3L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
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
        "\"venice_admin_.*\"");
    childClusters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test
  public void testNativeReplicationForBatchPush() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
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

    // Current version should become 1
    for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
      Assert.assertEquals(version, 1);
    }

    // Verify the data in Venice Store
    for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
      VeniceMultiClusterWrapper childDataCenter = childClusters.get(dataCenterIndex);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 100; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    }
  }
}
