package com.linkedin.venice.multicluster;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestMetadataOperationInMultiCluster {
  private static Logger logger = Logger.getLogger(TestMetadataOperationInMultiCluster.class);

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testCreateStoreAndVersionForMultiCluster() {
    int numberOfController = 3;
    int numberOfCluster = 2;
    String keySchema = "\"string\"";
    String valSchema = "\"string\"";
    try (VeniceMultiClusterWrapper multiClusterWrapper =
        ServiceFactory.getVeniceMultiClusterWrapper(numberOfCluster, numberOfController, 1, 1)) {
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertEquals(clusterNames.length, numberOfCluster, "Should created " + numberOfCluster + " clusters.");

      String clusterName = clusterNames[0];
      String secondCluster = clusterNames[1];

      try (VeniceControllerWrapper controllerWrapper = multiClusterWrapper.getRandomController();
          ControllerClient secondControllerClient = ControllerClient.constructClusterControllerClient(secondCluster, controllerWrapper.getControllerUrl())) {
        // controller client could talk to any controller and find the lead of the given cluster correclty.
        ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(clusterName, controllerWrapper.getControllerUrl());

        // Create store
        String storeName = "testCreateStoreAndVersionForMultiCluster";
        NewStoreResponse storeResponse = controllerClient.createNewStore(storeName, "test", keySchema, valSchema);
        Assert.assertFalse(storeResponse.isError(), "Should create a new store.");

        // Pickup the second cluster
        ;
        // Create store with the same name in this cluster
        storeResponse = controllerClient.createNewStore(storeName, "test", keySchema, valSchema);
        Assert.assertTrue(storeResponse.isError(), "Should not create the duplicated store even in another cluster.");

        // Create another store in this cluster
        String secondStoreName = "testCreateStoreAndVersionForMultiCluster_1";
        storeResponse = secondControllerClient.createNewStore(secondStoreName, "test", keySchema, valSchema);
        Assert.assertFalse(storeResponse.isError(), "Should create a new store.");

        VersionCreationResponse versionCreationResponse =
            controllerClient.requestTopicForWrites(storeName, 1000, Version.PushType.BATCH,
                Version.guidBasedDummyPushId(), false, true, false, Optional.empty(),
                Optional.empty(), Optional.empty(), false, -1);
        Assert.assertFalse(versionCreationResponse.isError());
        Assert.assertEquals(versionCreationResponse.getVersion(), 1);

        versionCreationResponse =
            secondControllerClient.requestTopicForWrites(secondStoreName, 1000, Version.PushType.BATCH,
                Version.guidBasedDummyPushId(), false, true, false, Optional.empty(),
                Optional.empty(), Optional.empty(), false, -1);
        Assert.assertFalse(versionCreationResponse.isError());
        Assert.assertEquals(versionCreationResponse.getVersion(), 1);

        // Create version in wrong cluster
        versionCreationResponse =
            controllerClient.requestTopicForWrites(secondStoreName, 1000, Version.PushType.BATCH,
                Version.guidBasedDummyPushId(), false, true, false, Optional.empty(),
                Optional.empty(), Optional.empty(), false, -1);
        Assert.assertTrue(versionCreationResponse.isError());
      }
    }
  }

  @Test
  public void testRunH2VInMultiCluster()
      throws Exception {
    int numberOfController = 3;
    int numberOfCluster = 2;
    try (VeniceMultiClusterWrapper multiClusterWrapper =
        ServiceFactory.getVeniceMultiClusterWrapper(numberOfCluster, numberOfController, 1, 1)) {
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      String storeNameSuffix = "-testStore";
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);

      Map<String, Properties> propertiesMap = new HashMap<>();
      for (String clusterName : clusterNames) {
        String storeName = clusterName + storeNameSuffix;
        // Use th first cluster in config, and test could h2v find the correct cluster.
        Properties h2vProperties = defaultH2VProps(multiClusterWrapper.getRandomController().getControllerUrl(), inputDirPath, storeName);
        propertiesMap.put(clusterName, h2vProperties);
        Schema keySchema = recordSchema.getField(h2vProperties.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema();
        Schema valueSchema =
            recordSchema.getField(h2vProperties.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema();

        try (ControllerClient controllerClient =
            ControllerClient.constructClusterControllerClient(clusterName, multiClusterWrapper.getRandomController().getControllerUrl())) {
          controllerClient.createNewStore(storeName, "test", keySchema.toString(), valueSchema.toString());
          ControllerResponse controllerResponse = controllerClient.updateStore(
              h2vProperties.getProperty(VENICE_STORE_NAME_PROP),
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

          Assert.assertFalse(controllerResponse.isError());
        }
      }

      for (String clusterName : clusterNames) {
        Properties properties = propertiesMap.get(clusterName);
        properties.setProperty(VenicePushJob.PBNJ_ENABLE, "true");
        properties.setProperty(VenicePushJob.PBNJ_ROUTER_URL_PROP, multiClusterWrapper.getClusters().get(clusterName).getRandomRouterURL());
        runH2V(properties, 1,
            ControllerClient.constructClusterControllerClient(clusterName, multiClusterWrapper.getRandomController().getControllerUrl()));
      }
    }
  }

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, ControllerClient controllerClient)
      throws Exception {

    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("job-" + expectedVersionNumber);
    // job will talk to any controller to do cluster discover then do the push.
    try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () ->
          controllerClient.getStore((String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
      logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
    }
  }
}
