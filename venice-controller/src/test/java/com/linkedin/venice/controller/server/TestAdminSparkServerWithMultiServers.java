package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Separate the tests from {@link TestAdminSparkServer}, because we need start more storage nodes to execute the
 * resource with bunch of partitions.
 * TODO: Since {@link TestAdminSparkServer} has adapted to have multi-servers. It's better to merge test cases.
 */
@Test
public class TestAdminSparkServerWithMultiServers {
  private static final int TEST_TIMEOUT = 20 * Time.MS_PER_SECOND;
  private static final int STORAGE_NODE_COUNT = 3;

  private VeniceClusterWrapper cluster;
  private ControllerClient controllerClient;

  @BeforeClass
  public void setUp() {
    cluster = ServiceFactory.getVeniceCluster(1, STORAGE_NODE_COUNT, 0);
    controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
  }

  @AfterClass
  public void tearDown() {
    IOUtils.closeQuietly(controllerClient);
    IOUtils.closeQuietly(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldListStores() {
    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) { //add 10 stores;
      storeNames.add(cluster.getNewStore(TestUtils.getUniqueString("venice-store")).getName());
    }

    MultiStoreResponse storeResponse = controllerClient.queryStoreList();
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore : storeNames) {
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldSendEmptyPushAndWait() {
    // Create a new store
    String storeName = cluster.getNewStore(TestUtils.getUniqueString("venice-store")).getName();
    // Send an empty push
    try{
      ControllerResponse response = controllerClient.sendEmptyPushAndWait(storeName,
          TestUtils.getUniqueString("emptyPushId"), 10000, TEST_TIMEOUT);
      Assert.assertFalse(response.isError(), "Received error response on empty push:" + response.getError());
    } catch(Exception e) {
      Assert.fail("Could not send empty push!!", e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void requestTopicIsIdempotent() {
    HashMap<String, Version.PushType> storeToType = new HashMap<>(2);
    storeToType.put(TestUtils.getUniqueString("BatchStore"), Version.PushType.BATCH);
    storeToType.put(TestUtils.getUniqueString("StreamStore"), Version.PushType.STREAM);

    String pushOne = TestUtils.getUniqueString("pushId");
    String pushTwo = pushOne;
    String pushThree = TestUtils.getUniqueString("pushId");

    for (String storeName : storeToType.keySet()) {
      cluster.getNewStore(storeName);

      // Stream
      if (storeToType.get(storeName).equals(Version.PushType.STREAM)) {
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(1000).setHybridOffsetLagThreshold(1000));
        controllerClient.emptyPush(storeName, TestUtils.getUniqueString("emptyPushId"), 10000);
      }

      // Both
      VersionCreationResponse responseOne =
          controllerClient.requestTopicForWrites(storeName, 1, storeToType.get(storeName), pushOne,
              false, false, Optional.empty());
      if (responseOne.isError()) {
        Assert.fail(responseOne.getError());
      }
      VersionCreationResponse responseTwo =
          controllerClient.requestTopicForWrites(storeName, 1, storeToType.get(storeName), pushTwo,
              false, false, Optional.empty());
      if (responseTwo.isError()) {
        Assert.fail(responseOne.getError());
      }

      Assert.assertEquals(responseOne.getKafkaTopic(), responseTwo.getKafkaTopic(),
          "Multiple requests for topics with the same pushId must return the same kafka topic");

      VersionCreationResponse responseThree =
          controllerClient.requestTopicForWrites(storeName, 1, storeToType.get(storeName), pushThree,
              false, false, Optional.empty());
      Assert.assertFalse(responseThree.isError(), "Controller should not allow concurrent push");
    }
  }

  /**
   * Multiple requests for a topic to write to for the same store.  Each request must provide the same version number
   * After the attempt, the version is made current so the next attempt generates a new version.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void requestTopicIsIdempotentWithConcurrency() {
    String storeName = TestUtils.getUniqueString("store");
    cluster.getNewStore(storeName);
    AtomicReference<String> errString = new AtomicReference<>();
    try {
      for (int i = 0; i < 5; i++) { // number of attempts
        String pushId = TestUtils.getUniqueString("pushId");
        final List<VersionCreationResponse> responses = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        int threadCount = 3; // number of concurrent requests
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int j = 0; j < threadCount; j++) {
          Thread t = requestTopicThread(pushId, storeName, responses, latch, errString);
          threads.add(t);
          t.setUncaughtExceptionHandler((t1, e) -> e.printStackTrace());
        }
        for (Thread t : threads) {
          t.start();
        }
        latch.await(10, TimeUnit.SECONDS);
        for (int j = 0; j < threadCount; j++) {
          if (responses.get(j).isError()) {
            Assert.fail(responses.get(j).getError());
          }
        }
        for (int j = 1; j < threadCount; j++) {
          Assert.assertEquals(responses.get(0).getKafkaTopic(), responses.get(j).getKafkaTopic(),
              "Idempotent topic requests failed under concurrency on attempt " + i + ".  If this test ever fails, investigate! Don't just run it again and hope it passes");
        }
        //close the new version so the next iteration gets a new version.
        controllerClient.writeEndOfPush(storeName, responses.get(0).getVersion());
        while (controllerClient.getStore(storeName).getStore().getCurrentVersion() < responses.get(0).getVersion()) {
          Utils.sleep(200);
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Captured message: " + errString.get());
    }
  }

  private Thread requestTopicThread(String pushId, String storeName, List<VersionCreationResponse> output, CountDownLatch latch, AtomicReference<String> errString) {
    return new Thread(() -> {
      final VersionCreationResponse vcr = new VersionCreationResponse();
      try {
        VersionCreationResponse thisVcr = controllerClient.requestTopicForWrites(storeName, 1, Version.PushType.BATCH, pushId,
            false, false, Optional.empty());
        vcr.setKafkaTopic(thisVcr.getKafkaTopic());
        vcr.setVersion(thisVcr.getVersion());
      } catch (Throwable t) {
        errString.set(t.getMessage());
        vcr.setError(t.getMessage());
      } finally {
        output.add(vcr);
        latch.countDown();
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void endOfPushEndpointTriggersVersionSwap() {
    String storeName = TestUtils.getUniqueString("store");
    String pushId = TestUtils.getUniqueString("pushId");
    cluster.getNewStore(storeName);
    StoreResponse freshStore = controllerClient.getStore(storeName);
    int oldVersion = freshStore.getStore().getCurrentVersion();
    VersionCreationResponse versionResponse = controllerClient.requestTopicForWrites(storeName, 1, Version.PushType.BATCH, pushId,
        false, true, Optional.empty());
    int newVersion = versionResponse.getVersion();
    Assert.assertNotEquals(newVersion, oldVersion, "Requesting a new version must not return the current version number");
    controllerClient.writeEndOfPush(storeName, newVersion);
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), newVersion, "Writing end of push must flip the version to current");
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanRemoveNodeFromCluster() {
    Admin admin = cluster.getMasterVeniceController().getVeniceAdmin();
    VeniceServerWrapper server = cluster.getVeniceServers().get(0);
    String nodeId = Utils.getHelixNodeIdentifier(server.getPort());
    // Trying to remove a live node.
    ControllerResponse response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertTrue(response.isError(), "Node is still connected to cluster, could not be removed.");

    // Remove a disconnected node.
    cluster.stopVeniceServer(server.getPort());
    response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertFalse(response.isError(), "Node is already disconnected, could be removed.");
    Assert.assertFalse(admin.getStorageNodes(cluster.getClusterName()).contains(nodeId),
        "Node should be removed from the cluster.");
  }
}
