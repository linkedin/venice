package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
    controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldListStores() {
    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) { // add 10 stores;
      storeNames.add(cluster.getNewStore(Utils.getUniqueString("venice-store")).getName());
    }

    MultiStoreResponse storeResponse = controllerClient.queryStoreList();
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore: storeNames) {
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }

  public void testListStoreWithConfigFilter() {
    // Add a store with native replication enabled
    String nativeReplicationEnabledStore = Utils.getUniqueString("native-replication-store");
    NewStoreResponse newStoreResponse =
        controllerClient.createNewStore(nativeReplicationEnabledStore, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());
    ControllerResponse updateStoreResponse = controllerClient
        .updateStore(nativeReplicationEnabledStore, new UpdateStoreQueryParams().setNativeReplicationEnabled(true));
    Assert.assertFalse(updateStoreResponse.isError());

    // Add a store with incremental push enabled
    String incrementalPushEnabledStore = Utils.getUniqueString("incremental-push-store");
    newStoreResponse = controllerClient.createNewStore(incrementalPushEnabledStore, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());
    updateStoreResponse = controllerClient.updateStore(
        incrementalPushEnabledStore,
        new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
            .setHybridOffsetLagThreshold(10L)
            .setHybridRewindSeconds(1L));
    Assert.assertFalse(updateStoreResponse.isError());

    // List stores that have native replication enabled
    MultiStoreResponse multiStoreResponse =
        controllerClient.queryStoreList(false, Optional.of("nativeReplicationEnabled"), Optional.of("true"));
    Assert.assertFalse(multiStoreResponse.isError());
    Assert.assertEquals(multiStoreResponse.getStores().length, 1);
    Assert.assertEquals(multiStoreResponse.getStores()[0], nativeReplicationEnabledStore);

    // List stores that have incremental push enabled
    multiStoreResponse =
        controllerClient.queryStoreList(false, Optional.of("incrementalPushEnabled"), Optional.of("true"));
    Assert.assertFalse(multiStoreResponse.isError());
    Assert.assertEquals(multiStoreResponse.getStores().length, 1);
    Assert.assertEquals(multiStoreResponse.getStores()[0], incrementalPushEnabledStore);

    // Add a store with hybrid config enabled and the DataReplicationPolicy is non-aggregate
    String hybridNonAggregateStore = Utils.getUniqueString("hybrid-non-aggregate");
    newStoreResponse = controllerClient.createNewStore(hybridNonAggregateStore, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());
    updateStoreResponse = controllerClient.updateStore(
        hybridNonAggregateStore,
        new UpdateStoreQueryParams().setHybridRewindSeconds(100)
            .setHybridOffsetLagThreshold(10)
            .setHybridDataReplicationPolicy(DataReplicationPolicy.NON_AGGREGATE));
    Assert.assertFalse(updateStoreResponse.isError());

    // Add a store with hybrid config enabled and the DataReplicationPolicy is aggregate
    String hybridAggregateStore = Utils.getUniqueString("hybrid-aggregate");
    newStoreResponse = controllerClient.createNewStore(hybridAggregateStore, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());
    updateStoreResponse = controllerClient.updateStore(
        hybridAggregateStore,
        new UpdateStoreQueryParams().setHybridRewindSeconds(100)
            .setHybridOffsetLagThreshold(10)
            .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE));
    Assert.assertFalse(updateStoreResponse.isError());

    // List stores that have hybrid config enabled
    multiStoreResponse = controllerClient.queryStoreList(false, Optional.of("hybridConfig"), Optional.of("true"));
    Assert.assertFalse(multiStoreResponse.isError());
    // Don't check the size of the return store list since the cluster is shared by all test cases.
    Set<String> hybridStoresSet = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    Assert.assertTrue(hybridStoresSet.contains(hybridAggregateStore));
    Assert.assertTrue(hybridStoresSet.contains(hybridNonAggregateStore));
    Assert.assertFalse(hybridStoresSet.contains(nativeReplicationEnabledStore));
    Assert.assertTrue(hybridStoresSet.contains(incrementalPushEnabledStore));

    // List hybrid stores that are on non-aggregate mode
    multiStoreResponse =
        controllerClient.queryStoreList(false, Optional.of("dataReplicationPolicy"), Optional.of("NON_AGGREGATE"));
    Assert.assertFalse(multiStoreResponse.isError());
    // Don't check the size of the return store list since the cluster is shared by all test cases.
    Set<String> nonAggHybridStoresSet = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    Assert.assertFalse(nonAggHybridStoresSet.contains(hybridAggregateStore));
    Assert.assertTrue(nonAggHybridStoresSet.contains(hybridNonAggregateStore));
    Assert.assertFalse(nonAggHybridStoresSet.contains(nativeReplicationEnabledStore));
    Assert.assertTrue(nonAggHybridStoresSet.contains(incrementalPushEnabledStore));

    // List hybrid stores that are on aggregate mode
    multiStoreResponse =
        controllerClient.queryStoreList(false, Optional.of("dataReplicationPolicy"), Optional.of("AGGREGATE"));
    Assert.assertFalse(multiStoreResponse.isError());
    // Don't check the size of the return store list since the cluster is shared by all test cases.
    Set<String> aggHybridStoresSet = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    Assert.assertTrue(aggHybridStoresSet.contains(hybridAggregateStore));
    Assert.assertFalse(aggHybridStoresSet.contains(hybridNonAggregateStore));
    Assert.assertFalse(aggHybridStoresSet.contains(nativeReplicationEnabledStore));
    Assert.assertFalse(aggHybridStoresSet.contains(incrementalPushEnabledStore));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldSendEmptyPushAndWait() {
    // Create a new store
    String storeName = cluster.getNewStore(Utils.getUniqueString("venice-store")).getName();
    // Send an empty push
    try {
      ControllerResponse response =
          controllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("emptyPushId"), 10000, TEST_TIMEOUT);
      Assert.assertFalse(response.isError(), "Received error response on empty push:" + response.getError());
    } catch (Exception e) {
      Assert.fail("Could not send empty push!!", e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldCreateStoreWithParameters() {
    String storeName = Utils.getUniqueString("venice-store");
    try {
      UpdateStoreQueryParams updateStore = new UpdateStoreQueryParams().setHybridRewindSeconds(1000)
          .setHybridOffsetLagThreshold(1000)
          .setHybridStoreOverheadBypass(true)
          .setEnableWrites(true)
          .setOwner("Napolean");
      ControllerResponse response =
          controllerClient.createNewStoreWithParameters(storeName, "The_Doge", "\"string\"", "\"string\"", updateStore);
      Assert.assertFalse(response.isError(), "Received error response on store creation:" + response.getError());
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(store.getOwner(), "Napolean");
      Assert.assertEquals(store.getHybridStoreConfig().getRewindTimeInSeconds(), 1000);
    } catch (Exception e) {
      Assert.fail("Could not create new Store with Exception!!", e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientShouldCreateStoreWithParametersAndNotDeleteItIfItExists() {
    String storeName = Utils.getUniqueString("venice-store");
    try {
      UpdateStoreQueryParams updateStore = new UpdateStoreQueryParams().setHybridRewindSeconds(1000)
          .setHybridOffsetLagThreshold(1000)
          .setHybridStoreOverheadBypass(true)
          .setEnableWrites(true)
          .setOwner("Napolean");
      ControllerResponse response =
          controllerClient.createNewStoreWithParameters(storeName, "The_Doge", "\"string\"", "\"string\"", updateStore);
      Assert.assertFalse(response.isError(), "Received error response on store creation:" + response.getError());
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(store.getOwner(), "Napolean");
      Assert.assertEquals(store.getHybridStoreConfig().getRewindTimeInSeconds(), 1000);

      // Create it again, and check to make sure it exists
      updateStore = new UpdateStoreQueryParams().setHybridRewindSeconds(1000)
          .setHybridOffsetLagThreshold(1000)
          .setHybridStoreOverheadBypass(true)
          .setEnableWrites(true)
          .setOwner("Napolean");
      response =
          controllerClient.createNewStoreWithParameters(storeName, "The_Doge", "\"string\"", "\"string\"", updateStore);
      Assert.assertTrue(response.isError(), "No Error Received!!!!!");
      store = controllerClient.getStore(storeName).getStore();
      Assert.assertNotNull(store, "Store unreadable!!  It may no longer exist!");
      Assert.assertEquals(store.getOwner(), "Napolean");
      Assert.assertEquals(store.getHybridStoreConfig().getRewindTimeInSeconds(), 1000);

    } catch (Exception e) {
      Assert.fail("Could not create new Store with Exception!!", e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void requestTopicIsIdempotent() {
    HashMap<String, Version.PushType> storeToType = new HashMap<>(2);
    storeToType.put(Utils.getUniqueString("BatchStore"), Version.PushType.BATCH);
    storeToType.put(Utils.getUniqueString("StreamStore"), Version.PushType.STREAM);

    String pushOne = Utils.getUniqueString("pushId");
    String pushTwo = pushOne;
    String pushThree = Utils.getUniqueString("pushId");

    for (Map.Entry<String, Version.PushType> entry: storeToType.entrySet()) {
      String storeName = entry.getKey();
      Version.PushType pushType = entry.getValue();
      cluster.getNewStore(storeName);

      // Stream
      if (pushType.equals(Version.PushType.STREAM)) {
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1000).setHybridOffsetLagThreshold(1000));
        controllerClient.emptyPush(storeName, Utils.getUniqueString("emptyPushId"), 10000);
      }

      // Both
      VersionCreationResponse responseOne = controllerClient.requestTopicForWrites(
          storeName,
          1,
          pushType,
          pushOne,
          true,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      if (responseOne.isError()) {
        Assert.fail(responseOne.getError());
      }
      VersionCreationResponse responseTwo = controllerClient.requestTopicForWrites(
          storeName,
          1,
          pushType,
          pushTwo,
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      if (responseTwo.isError()) {
        Assert.fail(responseOne.getError());
      }

      Assert.assertEquals(
          responseOne.getKafkaTopic(),
          responseTwo.getKafkaTopic(),
          "Multiple requests for topics with the same pushId must return the same kafka topic");

      VersionCreationResponse responseThree = controllerClient.requestTopicForWrites(
          storeName,
          1,
          pushType,
          pushThree,
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertFalse(responseThree.isError(), "Controller should not allow concurrent push");
    }
  }

  /**
   * Multiple requests for a topic to write to for the same store.  Each request must provide the same version number
   * After the attempt, the version is made current so the next attempt generates a new version.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void requestTopicIsIdempotentWithConcurrency() {
    String storeName = Utils.getUniqueString("store");
    cluster.getNewStore(storeName);
    AtomicReference<String> errString = new AtomicReference<>();
    try {
      for (int i = 0; i < 5; i++) { // number of attempts
        String pushId = Utils.getUniqueString("pushId");
        final List<VersionCreationResponse> responses = new ArrayList<>();

        int threadCount = 3; // number of concurrent requests
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Thread> threads = new ArrayList<>();
        try {
          for (int j = 0; j < threadCount; j++) {
            Thread t = requestTopicThread(pushId, storeName, responses, latch, errString);
            threads.add(t);
            t.setUncaughtExceptionHandler((t1, e) -> e.printStackTrace());
          }
          for (Thread t: threads) {
            t.start();
          }
          Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
          for (int j = 0; j < threadCount; j++) {
            if (responses.get(j).isError()) {
              Assert.fail(responses.get(j).getError());
            }
          }
          for (int j = 1; j < threadCount; j++) {
            Assert.assertEquals(
                responses.get(0).getKafkaTopic(),
                responses.get(j).getKafkaTopic(),
                "Idempotent topic requests failed under concurrency on attempt " + i
                    + ".  If this test ever fails, investigate! Don't just run it again and hope it passes");
          }
          // close the new version so the next iteration gets a new version.
          controllerClient.writeEndOfPush(storeName, responses.get(0).getVersion());
          while (controllerClient.getStore(storeName).getStore().getCurrentVersion() < responses.get(0).getVersion()
              && Utils.sleep(200)) {
          }
        } finally {
          for (Thread t: threads) {
            TestUtils.shutdownThread(t);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Captured message: " + errString.get());
    }
  }

  private Thread requestTopicThread(
      String pushId,
      String storeName,
      List<VersionCreationResponse> output,
      CountDownLatch latch,
      AtomicReference<String> errString) {
    return new Thread(() -> {
      final VersionCreationResponse vcr = new VersionCreationResponse();
      try {
        VersionCreationResponse thisVcr = controllerClient.requestTopicForWrites(
            storeName,
            1,
            Version.PushType.BATCH,
            pushId,
            true,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
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
    String storeName = Utils.getUniqueString("store");
    String pushId = Utils.getUniqueString("pushId");
    cluster.getNewStore(storeName);
    StoreResponse freshStore = controllerClient.getStore(storeName);
    int oldVersion = freshStore.getStore().getCurrentVersion();
    VersionCreationResponse versionResponse = controllerClient.requestTopicForWrites(
        storeName,
        1,
        Version.PushType.BATCH,
        pushId,
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    int newVersion = versionResponse.getVersion();
    Assert
        .assertNotEquals(newVersion, oldVersion, "Requesting a new version must not return the current version number");
    controllerClient.writeEndOfPush(storeName, newVersion);
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(
          storeResponse.getStore().getCurrentVersion(),
          newVersion,
          "Writing end of push must flip the version to current");
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanRemoveNodeFromCluster() {
    Admin admin = cluster.getLeaderVeniceController().getVeniceAdmin();
    VeniceServerWrapper server = cluster.getVeniceServers().get(0);
    String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());
    // Trying to remove a live node.
    ControllerResponse response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertTrue(response.isError(), "Node is still connected to cluster, could not be removed.");

    // Remove a disconnected node.
    cluster.stopVeniceServer(server.getPort());
    response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertFalse(response.isError(), "Node is already disconnected, could be removed.");
    Assert.assertFalse(
        admin.getStorageNodes(cluster.getClusterName()).contains(nodeId),
        "Node should be removed from the cluster.");
  }
}
