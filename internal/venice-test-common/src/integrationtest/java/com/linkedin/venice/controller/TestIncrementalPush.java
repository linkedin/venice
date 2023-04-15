package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * TestIncrementalPush tests the incremental push feature
 */
public class TestIncrementalPush {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;
  int numberOfServer = 3;

  @BeforeMethod
  public void setUp() {
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfServers(numberOfServer)
            .replicationFactor(replicaFactor)
            .partitionSize(partitionSize)
            .minActiveReplica(replicaFactor - 1)
            .build());
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 2 * Time.MS_PER_MINUTE, invocationCount = 20)
  public void testGetOfflineStatusIncrementalPush() {
    String storeName = Utils.getUniqueString("testIncPushStore");
    int partitionCount = 2;
    long storageQuota = (long) partitionCount * partitionSize;
    cluster.getNewStore(storeName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setIncrementalPushEnabled(true)
        .setHybridRewindSeconds(1)
        .setHybridOffsetLagThreshold(1)
        .setStorageQuotaInByte(storageQuota)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(3);
    cluster.updateStore(storeName, params);

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
        1,
        TimeUnit.MINUTES,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic1, Optional.of(incPushV1), null)
            .getExecutionStatus()
            .equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED));

    // store version 2
    VersionCreationResponse v2Response = cluster.getNewVersion(storeName);
    String versionTopic2 = v2Response.getKafkaTopic();
    // push version 2 to completion
    VeniceWriter<String, String, byte[]> veniceWriterVt2 = cluster.getVeniceWriter(versionTopic2);
    veniceWriterVt2.broadcastStartOfPush(new HashMap<>());
    veniceWriterVt2.broadcastEndOfPush(new HashMap<>());

    TestUtils.waitForNonDeterministicCompletion(
        1,
        TimeUnit.MINUTES,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic2, Optional.of(incPushV1), null)
            .getExecutionStatus()
            .equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED));

    String incPush2 = "incPush2";
    veniceWriterVt2.broadcastStartOfIncrementalPush(incPush2, new HashMap<>());
    // validate current version query works
    TestUtils.waitForNonDeterministicCompletion(
        1,
        TimeUnit.MINUTES,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), versionTopic2, Optional.of(incPush2), null)
            .getExecutionStatus()
            .equals(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED));
  }
}
