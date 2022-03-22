package com.linkedin.venice.endToEnd;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ClusterStaleDataAuditResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.venice.utils.TestPushUtils.*;

public class TestStaleDataVisibility {
  private static final Logger LOGGER = Logger.getLogger(TestMultiDataCenterPush.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
      // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<List<VeniceControllerWrapper>> childControllers;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(ConfigKeys.CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD, "true");
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
      NUMBER_OF_CHILD_DATACENTERS, NUMBER_OF_CLUSTERS, 1, 1, 1, 1,
      1, Optional.empty(), Optional.of(childControllerProperties), Optional.of(new VeniceProperties(serverProperties)), false,
      MirrorMakerWrapper.DEFAULT_TOPIC_ALLOWLIST);

    childClusters = multiColoMultiClusterWrapper.getClusters();
    childControllers = childClusters.stream()
      .map(veniceClusterWrapper -> veniceClusterWrapper.getControllers()
      .values()
          .stream()
          .collect(Collectors.toList()))
      .collect(Collectors.toList());
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    LOGGER.info("parentControllers: " + parentControllers.stream()
        .map(c -> c.getControllerUrl())
      .collect(Collectors.joining(", ")));

    int i = 0;
    for (VeniceMultiClusterWrapper multiClusterWrapper : childClusters) {
      LOGGER.info("childCluster" + i++ + " controllers: " + multiClusterWrapper.getControllers()
        .values()
        .stream()
        .map(c -> c.getControllerUrl())
        .collect(Collectors.joining(", ")));
    }
  }

  @AfterClass
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetClusterStaleStores() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();

    console.log(storeName + " is our store name ASDY1");

    // create a store via parent controller url
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();
    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();
    }

    try (ControllerClient controllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {

      // the store should not be appearing in the stale data audit
      ClusterStaleDataAuditResponse emptyResponse = controllerClient.getClusterStaleStores(clusterName, parentController.getControllerUrl(), Optional.empty());
      Assert.assertFalse(emptyResponse.getAuditMap().get(storeName).getStaleRegions().containsKey(storeName));

      // get single child controller, empty push to it
      VeniceControllerWrapper childController = childControllers.get(0).get(0);
      Properties props2 = defaultH2VProps(childController.getControllerUrl(), inputDirPath, storeName);
      try (VenicePushJob job = new VenicePushJob("Test push job", props2)) {
        job.run();
      }

      // store should now appear as stale
      ClusterStaleDataAuditResponse response = controllerClient.getClusterStaleStores(clusterName, parentController.getControllerUrl(), Optional.empty());
      for (Map.Entry<String, StoreDataAudit> entry : response.getAuditMap().entrySet()) {
        LOGGER.error("ASDF123" + entry.getValue().toString());
      }
      Assert.assertTrue(response.getAuditMap().get(storeName).getStaleRegions().containsKey(storeName));

      //test store health check
      StoreHealthAuditResponse healthResponse = controllerClient.listStorePushInfo(clusterName, parentController.getControllerUrl(), storeName);
      Assert.assertTrue(response.getAuditMap().containsKey(healthResponse.getStoreName()));
      Map<String, StoreInfo> auditMapEntry = response.getAuditMap().get(healthResponse.getStoreName()).getStaleRegions();
      for (Map.Entry<String, StoreInfo> entry : auditMapEntry.entrySet())
        if (entry.getValue().getName() == storeName)
          Assert.assertTrue(healthResponse.getRegionsWithStaleData().contains(entry.getKey())); // verify that the same regions are stale across both responses for the same store
    }
  }
}