package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.THROUGHPUT_QUOTA_IN_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.THROUGHPUT_QUOTA_IN_RECORDS;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreUpdateHandlerIntegrationTest {
  private static final long TEST_TIMEOUT_MS = 2 * Time.MS_PER_MINUTE;
  private static final long UPDATED_THROUGHPUT_QUOTA_IN_BYTES = 10 * 1024 * 1024;
  private static final long UPDATED_THROUGHPUT_QUOTA_IN_RECORDS = 5000;

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testStoreUpdateHandlerRetriesWithFinalReadOnlySnapshot() throws InterruptedException {
    String storeName = Utils.getUniqueString("store-update-handler");
    String originalOwner = "test-owner";
    RetryingStoreUpdateHandler storeUpdateHandler = new RetryingStoreUpdateHandler(
        storeName,
        UPDATED_THROUGHPUT_QUOTA_IN_BYTES,
        UPDATED_THROUGHPUT_QUOTA_IN_RECORDS);
    AtomicInteger childHandlerInvocationCount = new AtomicInteger();

    Properties parentControllerProperties = new Properties();
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, Boolean.FALSE.toString());
    parentControllerProperties
        .setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, Boolean.FALSE.toString());
    parentControllerProperties.put(VeniceControllerWrapper.STORE_UPDATE_HANDLER, storeUpdateHandler);
    Properties childControllerProperties = new Properties();
    childControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, Boolean.FALSE.toString());
    childControllerProperties
        .setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, Boolean.FALSE.toString());
    childControllerProperties.put(
        VeniceControllerWrapper.STORE_UPDATE_HANDLER,
        (StoreUpdateHandler) (clusterName, store, updatedConfigs) -> childHandlerInvocationCount.incrementAndGet());

    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(0)
            .numberOfRouters(0)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProperties)
            .childControllerProperties(childControllerProperties)
            .build();

    try (VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options)) {
      String clusterName = venice.getClusterNames()[0];
      String childControllerUrl = venice.getChildRegions().get(0).getControllerConnectString();
      try (
          ControllerClient parentControllerClient =
              new ControllerClient(clusterName, venice.getControllerConnectString());
          ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl)) {
        NewStoreResponse newStoreResponse =
            parentControllerClient.createNewStore(storeName, originalOwner, "\"string\"", "\"string\"");
        Assert.assertFalse(newStoreResponse.isError(), newStoreResponse.getError());
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> Assert.assertFalse(childControllerClient.getStore(storeName).isError()));

        ControllerResponse updateStoreResponse = parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setThroughputQuotaInBytes(UPDATED_THROUGHPUT_QUOTA_IN_BYTES)
                .setThroughputQuotaInRecords(UPDATED_THROUGHPUT_QUOTA_IN_RECORDS));
        Assert.assertFalse(updateStoreResponse.isError(), updateStoreResponse.getError());
        Assert.assertTrue(
            storeUpdateHandler.awaitSuccessfulInvocation(30, TimeUnit.SECONDS),
            "The store update handler did not succeed after its first-attempt failure");

        Store callbackStore = storeUpdateHandler.getLatestStore();
        Assert.assertTrue(storeUpdateHandler.getInvocationCount() >= 2);
        Assert.assertEquals(storeUpdateHandler.getLatestClusterName(), clusterName);
        Assert.assertEquals(callbackStore.getName(), storeName);
        Assert.assertEquals(callbackStore.getOwner(), originalOwner);
        Assert.assertEquals(callbackStore.getThroughputQuotaInBytes(), UPDATED_THROUGHPUT_QUOTA_IN_BYTES);
        Assert.assertEquals(callbackStore.getThroughputQuotaInRecords(), UPDATED_THROUGHPUT_QUOTA_IN_RECORDS);
        Assert.assertTrue(storeUpdateHandler.receivedOnlyReadOnlyStores());
        Assert.assertTrue(storeUpdateHandler.receivedOnlyImmutableUpdatedConfigs());
        Set<String> expectedUpdatedConfigs =
            new HashSet<>(Arrays.asList(THROUGHPUT_QUOTA_IN_BYTES, THROUGHPUT_QUOTA_IN_RECORDS));
        Assert.assertTrue(
            storeUpdateHandler.getReceivedUpdatedConfigs()
                .stream()
                .allMatch(updatedConfigs -> updatedConfigs.equals(expectedUpdatedConfigs)));

        String barrierOwner = "owner-after-update";
        ControllerResponse setOwnerResponse = parentControllerClient.setStoreOwner(storeName, barrierOwner);
        Assert.assertFalse(setOwnerResponse.isError(), setOwnerResponse.getError());
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreInfo childStore = childControllerClient.getStore(storeName).getStore();
          Assert.assertEquals(childStore.getOwner(), barrierOwner);
          Assert.assertEquals(childStore.getThroughputQuotaInBytes(), UPDATED_THROUGHPUT_QUOTA_IN_BYTES);
          Assert.assertEquals(childStore.getThroughputQuotaInRecords(), UPDATED_THROUGHPUT_QUOTA_IN_RECORDS);
        });

        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getThroughputQuotaInBytes(), UPDATED_THROUGHPUT_QUOTA_IN_BYTES);
        Assert.assertEquals(parentStore.getThroughputQuotaInRecords(), UPDATED_THROUGHPUT_QUOTA_IN_RECORDS);
        Assert.assertTrue(storeUpdateHandler.getInvocationCount() >= 2);
        Assert.assertEquals(childHandlerInvocationCount.get(), 0);
      }
    }
  }

  private static final class RetryingStoreUpdateHandler implements StoreUpdateHandler {
    private final String targetStoreName;
    private final long targetThroughputQuotaInBytes;
    private final long targetThroughputQuotaInRecords;
    private final AtomicInteger invocationCount = new AtomicInteger();
    private final AtomicReference<String> latestClusterName = new AtomicReference<>();
    private final AtomicReference<Store> latestStore = new AtomicReference<>();
    private final AtomicBoolean receivedOnlyReadOnlyStores = new AtomicBoolean(true);
    private final AtomicBoolean receivedOnlyImmutableUpdatedConfigs = new AtomicBoolean(true);
    private final CopyOnWriteArrayList<Set<String>> receivedUpdatedConfigs = new CopyOnWriteArrayList<>();
    private final CountDownLatch successfulInvocation = new CountDownLatch(1);

    private RetryingStoreUpdateHandler(
        String targetStoreName,
        long targetThroughputQuotaInBytes,
        long targetThroughputQuotaInRecords) {
      this.targetStoreName = targetStoreName;
      this.targetThroughputQuotaInBytes = targetThroughputQuotaInBytes;
      this.targetThroughputQuotaInRecords = targetThroughputQuotaInRecords;
    }

    @Override
    public void handleStoreUpdate(String clusterName, Store store, Set<String> updatedConfigs) {
      if (!targetStoreName.equals(store.getName()) || store.getThroughputQuotaInBytes() != targetThroughputQuotaInBytes
          || store.getThroughputQuotaInRecords() != targetThroughputQuotaInRecords) {
        return;
      }

      latestClusterName.set(clusterName);
      latestStore.set(store);
      receivedUpdatedConfigs.add(updatedConfigs);
      try {
        store.setOwner("unexpected-mutation");
        receivedOnlyReadOnlyStores.set(false);
      } catch (UnsupportedOperationException expected) {
        // Expected for callback snapshots.
      }
      try {
        updatedConfigs.add("unexpected-config");
        receivedOnlyImmutableUpdatedConfigs.set(false);
      } catch (UnsupportedOperationException expected) {
        // Expected for callback config sets.
      }

      if (invocationCount.incrementAndGet() == 1) {
        throw new VeniceRetriableException("Expected first-attempt store update handler failure");
      }
      successfulInvocation.countDown();
    }

    private boolean awaitSuccessfulInvocation(long timeout, TimeUnit unit) throws InterruptedException {
      return successfulInvocation.await(timeout, unit);
    }

    private int getInvocationCount() {
      return invocationCount.get();
    }

    private String getLatestClusterName() {
      return latestClusterName.get();
    }

    private Store getLatestStore() {
      return latestStore.get();
    }

    private boolean receivedOnlyReadOnlyStores() {
      return receivedOnlyReadOnlyStores.get();
    }

    private boolean receivedOnlyImmutableUpdatedConfigs() {
      return receivedOnlyImmutableUpdatedConfigs.get();
    }

    private List<Set<String>> getReceivedUpdatedConfigs() {
      return new ArrayList<>(receivedUpdatedConfigs);
    }
  }
}
