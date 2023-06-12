package com.linkedin.venice.multicluster;

import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMetadataOperationInMultiCluster {
  private static final Logger LOGGER = LogManager.getLogger(TestMetadataOperationInMultiCluster.class);

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testCreateStoreAndVersionForMultiCluster() {
    String keySchema = "\"string\"";
    String valSchema = "\"string\"";

    VeniceMultiClusterCreateOptions options = new VeniceMultiClusterCreateOptions.Builder(2).numberOfControllers(3)
        .numberOfServers(1)
        .numberOfRouters(1)
        .build();
    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(options)) {
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      Assert.assertEquals(
          clusterNames.length,
          options.getNumberOfClusters(),
          "Should created " + options.getNumberOfClusters() + " clusters.");

      String clusterName = clusterNames[0];
      String secondCluster = clusterNames[1];

      try (VeniceControllerWrapper controllerWrapper = multiClusterWrapper.getRandomController();
          ControllerClient secondControllerClient =
              ControllerClient.constructClusterControllerClient(secondCluster, controllerWrapper.getControllerUrl())) {
        // controller client could talk to any controller and find the leader of the given cluster correctly.
        ControllerClient controllerClient =
            ControllerClient.constructClusterControllerClient(clusterName, controllerWrapper.getControllerUrl());

        // Create store
        String storeName = "testCreateStoreAndVersionForMultiCluster";
        NewStoreResponse storeResponse = controllerClient.createNewStore(storeName, "test", keySchema, valSchema);
        Assert.assertFalse(storeResponse.isError(), "Should create a new store.");

        // Create store with the same name in this cluster
        storeResponse = controllerClient.createNewStore(storeName, "test", keySchema, valSchema);
        Assert.assertTrue(storeResponse.isError(), "Should not create the duplicated store even in another cluster.");

        // Create another store in this cluster
        String secondStoreName = "testCreateStoreAndVersionForMultiCluster_1";
        storeResponse = secondControllerClient.createNewStore(secondStoreName, "test", keySchema, valSchema);
        Assert.assertFalse(storeResponse.isError(), "Should create a new store.");

        VersionCreationResponse versionCreationResponse = controllerClient.requestTopicForWrites(
            storeName,
            1000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
        Assert.assertFalse(versionCreationResponse.isError());
        Assert.assertEquals(versionCreationResponse.getVersion(), 1);

        versionCreationResponse = secondControllerClient.requestTopicForWrites(
            secondStoreName,
            1000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
        Assert.assertFalse(versionCreationResponse.isError());
        Assert.assertEquals(versionCreationResponse.getVersion(), 1);

        // Create version in wrong cluster
        versionCreationResponse = controllerClient.requestTopicForWrites(
            secondStoreName,
            1000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
        Assert.assertTrue(versionCreationResponse.isError());
      }
    }
  }

  @Test
  public void testRunVPJInMultiCluster() throws Exception {
    VeniceMultiClusterCreateOptions options = new VeniceMultiClusterCreateOptions.Builder(2).numberOfControllers(3)
        .numberOfServers(1)
        .numberOfRouters(1)
        .build();
    try (VeniceMultiClusterWrapper multiClusterWrapper = ServiceFactory.getVeniceMultiClusterWrapper(options)) {
      String[] clusterNames = multiClusterWrapper.getClusterNames();
      String storeNameSuffix = "-testStore";
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);

      Map<String, Properties> propertiesMap = new HashMap<>();
      for (String clusterName: clusterNames) {
        String storeName = clusterName + storeNameSuffix;
        // Use th first cluster in config, and test could h2v find the correct cluster.
        Properties vpjProperties =
            IntegrationTestPushUtils.defaultVPJProps(multiClusterWrapper, inputDirPath, storeName);
        propertiesMap.put(clusterName, vpjProperties);
        Schema keySchema = recordSchema.getField(VenicePushJob.DEFAULT_KEY_FIELD_PROP).schema();
        Schema valueSchema = recordSchema.getField(VenicePushJob.DEFAULT_VALUE_FIELD_PROP).schema();

        try (ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(
            clusterName,
            multiClusterWrapper.getRandomController().getControllerUrl())) {
          controllerClient.createNewStore(storeName, "test", keySchema.toString(), valueSchema.toString());
          ControllerResponse controllerResponse = controllerClient.updateStore(
              vpjProperties.getProperty(VENICE_STORE_NAME_PROP),
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

          Assert.assertFalse(controllerResponse.isError());
        }
      }

      for (String clusterName: clusterNames) {
        try (ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(
            clusterName,
            multiClusterWrapper.getRandomController().getControllerUrl())) {
          Properties properties = propertiesMap.get(clusterName);
          runVPJ(properties, 1, controllerClient);
        }
      }
    }
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient)
      throws Exception {

    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("job-" + expectedVersionNumber);
    // job will talk to any controller to do cluster discover then do the push.
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          5,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
      LOGGER.info("**TIME** VPJ {} takes {} ms.", expectedVersionNumber, (System.currentTimeMillis() - vpjStart));
    }
  }
}
