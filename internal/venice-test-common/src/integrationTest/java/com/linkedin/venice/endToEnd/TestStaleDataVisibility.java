package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ClusterStaleDataAuditResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStaleDataVisibility extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestStaleDataVisibility.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetClusterStaleStores() throws Exception {
    String clusterName = CLUSTER_NAME;
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String dc0ControllerUrls = childDatacenters.get(0).getControllerConnectString();

    // create a store via parent controller url
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();
    try (ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrls)) {
      String pushStatusStoreVersionName =
          Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
      String metaStoreVersionName =
          Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1);
      TestUtils
          .waitForNonDeterministicPushCompletion(pushStatusStoreVersionName, controllerClient, 1, TimeUnit.MINUTES);
      TestUtils.waitForNonDeterministicPushCompletion(metaStoreVersionName, controllerClient, 1, TimeUnit.MINUTES);
    }

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();
    }

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();
    }

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrls);
        ControllerClient dc0ControllerClient = new ControllerClient(clusterName, dc0ControllerUrls)) {
      // the store should not be appearing in the stale data audit
      ClusterStaleDataAuditResponse emptyResponse =
          parentControllerClient.getClusterStaleStores(clusterName, parentControllerUrls);
      Assert.assertFalse(emptyResponse.isError());
      Assert.assertFalse(emptyResponse.getAuditMap().containsKey(storeName));

      // get single child controller, rollback and delete a version. Revert the largest used version.
      assertCommand(dc0ControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setCurrentVersion(1)));
      assertCommand(dc0ControllerClient.deleteOldVersion(storeName, 2));
      assertCommand(
          dc0ControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setLargestUsedVersionNumber(1)));

      // store should now appear as stale
      ClusterStaleDataAuditResponse response =
          assertCommand(parentControllerClient.getClusterStaleStores(clusterName, parentControllerUrls));
      Assert.assertTrue(response.getAuditMap().containsKey(storeName));
      StoreDataAudit auditForStore = response.getAuditMap().get(storeName);
      Assert.assertEquals(auditForStore.getStaleRegions().size(), 1);
      Assert.assertEquals(auditForStore.getHealthyRegions().size(), 1);

      // test store health check
      StoreHealthAuditResponse healthResponse = parentControllerClient.listStorePushInfo(storeName, true);
      Map<String, StoreInfo> auditMapEntry = auditForStore.getStaleRegions();
      for (Map.Entry<String, StoreInfo> entry: auditMapEntry.entrySet()) {
        // verify that the same regions are stale across both responses for the same store.
        Assert.assertTrue(healthResponse.getRegionsWithStaleData().contains(entry.getKey()));
      }
    }
  }
}
