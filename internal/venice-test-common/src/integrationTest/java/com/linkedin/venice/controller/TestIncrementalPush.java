package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * TestIncrementalPush tests the incremental push feature
 */
public class TestIncrementalPush {
  private VeniceClusterWrapper cluster;
  private String storeName;
  private static final int PARTITION_SIZE = 1000;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    // set the system property to use the local controller
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfServers(3)
            .replicationFactor(3)
            .partitionSize(PARTITION_SIZE)
            .build());
  }

  @AfterClass(alwaysRun = true)
  public void tearDownClass() {
    cluster.close();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    storeName = Utils.getUniqueString("testIncPushStore");
    cluster.getNewStore(storeName);
    long storageQuota = 2L * PARTITION_SIZE;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(1)
        .setHybridOffsetLagThreshold(1)
        .setStorageQuotaInByte(storageQuota)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(3);
    cluster.updateStore(storeName, params);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    cluster.useControllerClient(controllerClient -> controllerClient.deleteStore(storeName));
  }

  @Test(timeOut = 2 * Time.MS_PER_MINUTE)
  public void testGetOfflineStatusIncrementalPush() {
    // store version 1
    VersionCreationResponse v1Response = cluster.getNewVersion(storeName);
    Assert.assertFalse(v1Response.isError());
    String versionTopic1 = v1Response.getKafkaTopic();
    // push version 1 to completion
    VeniceWriter<String, String, byte[]> veniceWriterVt1 = cluster.getVeniceWriter(versionTopic1);
    veniceWriterVt1.broadcastStartOfPush(new HashMap<>());
    veniceWriterVt1.broadcastEndOfPush(new HashMap<>());
    // write incremental push to store version 1
    String incPushV1 = "incPush1";
    veniceWriterVt1.broadcastStartOfIncrementalPush(incPushV1, new HashMap<>());
    veniceWriterVt1.broadcastEndOfIncrementalPush(incPushV1, new HashMap<>());

    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic1, Optional.of(incPushV1), null, null)
            .getExecutionStatus()
            .equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED));

    // store version 2
    VersionCreationResponse v2Response = cluster.getNewVersion(storeName);
    String versionTopic2 = v2Response.getKafkaTopic();
    // push version 2 to completion
    VeniceWriter<String, String, byte[]> veniceWriterVt2 = cluster.getVeniceWriter(versionTopic2);
    veniceWriterVt2.broadcastStartOfPush(new HashMap<>());
    veniceWriterVt2.broadcastEndOfPush(new HashMap<>());

    // make sure the store has 2 versions
    cluster.useControllerClient(controllerClient -> {
      StoreResponse storeResponse =
          assertCommand(controllerClient.getStore(storeName), "Store response should not be null");
      StoreInfo storeInfo = requireNonNull(storeResponse.getStore(), "Store should not be null");
      List<Version> storeVersions = requireNonNull(storeInfo.getVersions(), "Store versions should not be null");
      assertEquals(storeVersions.size(), 2, "Store should have 2 versions");
    });

    // even though current version does not contain the incremental push,
    // status from the previous version should be returned
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic2, Optional.of(incPushV1), null, null)
            .getExecutionStatus()
            .equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED));

    String incPush2 = "incPush2";
    veniceWriterVt2.broadcastStartOfIncrementalPush(incPush2, new HashMap<>());
    // validate current version query works
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic2, Optional.of(incPush2), null, null)
            .getExecutionStatus()
            .equals(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED));
  }
}
