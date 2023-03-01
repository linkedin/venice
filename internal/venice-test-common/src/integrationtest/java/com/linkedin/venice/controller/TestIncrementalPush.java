package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.services.ServiceFactory;
import com.linkedin.venice.services.VeniceClusterWrapper;
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


public class TestIncrementalPush {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;
  int numberOfServer = 3;

  @BeforeMethod
  public void setUp() {
    int numberOfController = 1;
    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(
        numberOfController,
        numberOfServer,
        numberOfRouter,
        replicaFactor,
        partitionSize,
        false,
        false);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testGetOfflineStatusIncrementalPush() {
    String storeName = Utils.getUniqueString("testIncPushStore");
    int partitionCount = 2;
    int dataSize = partitionCount * partitionSize;
    cluster.getNewStore(storeName);
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setIncrementalPushEnabled(true).setHybridRewindSeconds(1).setHybridOffsetLagThreshold(1);
    cluster.updateStore(storeName, params);

    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();
    // push version 1 to completion
    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // start inc push
    String incPushVerison = "incPush1";
    veniceWriter.broadcastStartOfIncrementalPush(incPushVerison, new HashMap<>());
    veniceWriter.broadcastEndOfIncrementalPush(incPushVerison, new HashMap<>());

    params = new UpdateStoreQueryParams();
    // set up store for inc push to rt

    cluster.updateStore(storeName, params);
    response = cluster.getNewVersion(storeName, dataSize);
    topicName = response.getKafkaTopic();

    // broadcast to VT
    veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    String finalTopicName1 = topicName;
    // validate it still returns valid status from backup version
    String finalIncPushVerison1 = incPushVerison;
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), finalTopicName1, Optional.of(finalIncPushVerison1))
            .getExecutionStatus()
            .equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED));

    incPushVerison = "incPush2";
    veniceWriter.broadcastStartOfIncrementalPush(incPushVerison, new HashMap<>());
    String finalTopicName = topicName;
    String finalIncPushVerison = incPushVerison;
    // validate current version query works
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), finalTopicName, Optional.of(finalIncPushVerison))
            .getExecutionStatus()
            .equals(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED));
  }
}
