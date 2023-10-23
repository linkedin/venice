package com.linkedin.venice.listener;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This test makes sure the registration/unregistration listener logic on the ReadQuotaEnforcementHandler
 * all works to keep the Enforcer up-to-date
 */
public class ReadQuotaEnforcementHandlerListenerTest {
  private String nodeId = "thisNodeId";

  @Test
  public void quotaEnforcementHandlerRegistersAsStoreChangeListener() {
    long storageNodeRcuCapacity = 100; // RCU per second
    HelixCustomizedViewOfflinePushRepository customizedViewRepository =
        mock(HelixCustomizedViewOfflinePushRepository.class);
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    AggServerQuotaUsageStats stats = mock(AggServerQuotaUsageStats.class);
    MetricsRepository metricsRepository = new MetricsRepository();

    List<StoreDataChangedListener> listeners = new ArrayList<>();
    doAnswer((invocation) -> {
      StoreDataChangedListener listener = invocation.getArgument(0);
      listeners.add(listener);
      return null;
    }).when(storeRepository).registerStoreDataChangedListener(any());

    ReadQuotaEnforcementHandler quotaEnforcer = new ReadQuotaEnforcementHandler(
        storageNodeRcuCapacity,
        storeRepository,
        CompletableFuture.completedFuture(customizedViewRepository),
        nodeId,
        stats,
        metricsRepository);

    Assert.assertEquals(listeners.get(0), quotaEnforcer);
  }

  @Test
  public void quotaEnforcementHandlerStaysUpToDateWithStoreChanges() {
    Set<String> registeredTopics = new HashSet<>();

    long storageNodeRcuCapacity = 100; // RCU per second
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    doAnswer((invocation) -> {
      String storeName = invocation.getArgument(0);
      return getDummyStore(storeName, Collections.EMPTY_LIST, 10); // only used for RCU call
    }).when(storeRepository).getStore(anyString());

    HelixCustomizedViewOfflinePushRepository customizedViewRepository =
        mock(HelixCustomizedViewOfflinePushRepository.class);

    doAnswer((invocation) -> {
      String topic = invocation.getArgument(0);
      registeredTopics.add(topic);
      return null;
    }).when(customizedViewRepository).subscribeRoutingDataChange(anyString(), any());

    doAnswer((invocation) -> {
      String topic = invocation.getArgument(0);
      registeredTopics.remove(topic);
      return null;
    }).when(customizedViewRepository).unSubscribeRoutingDataChange(anyString(), any());

    doAnswer((invocation) -> {
      String topic = invocation.getArgument(0);
      return getDummyPartitionAssignment(topic, nodeId, customizedViewRepository);
    }).when(customizedViewRepository).getPartitionAssignments(anyString());

    AggServerQuotaUsageStats stats = mock(AggServerQuotaUsageStats.class);
    MetricsRepository metricsRepository = new MetricsRepository();

    // Object under test
    ReadQuotaEnforcementHandler quotaEnforcer = new ReadQuotaEnforcementHandler(
        storageNodeRcuCapacity,
        storeRepository,
        CompletableFuture.completedFuture(customizedViewRepository),
        nodeId,
        stats,
        metricsRepository);

    // Add a store (call store created) verify all versions in buckets and in subscriptions
    Store store1 = getDummyStore("store1", Arrays.asList(new Integer[] { 1 }), 10);
    store1.setCurrentVersion(1);
    quotaEnforcer.handleStoreCreated(store1);
    assertTrue(
        registeredTopics.contains(Version.composeKafkaTopic(store1.getName(), 1)),
        "After adding a store with version 1, the throttler should be subscribed to updates for that topic");
    assertTrue(
        quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store1.getName(), 1)),
        "After adding a store with version 1, the throttler should have a bucket for that topic");

    // Add another store (call store created) verify all versions in buckets and in subscriptions

    List<Integer> versions = Arrays.asList(new Integer[] { 2, 3 });
    Store store2 = getDummyStore("store2", versions, 10);
    store2.setCurrentVersion(3);
    quotaEnforcer.handleStoreCreated(store2);
    assertTrue(
        registeredTopics.contains(Version.composeKafkaTopic(store2.getName(), 3)),
        "After adding a store with version " + 3 + ", the throttler should be subscribed to updates for that topic");
    assertTrue(
        quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store2.getName(), 3)),
        "After adding a store with version " + 3 + ", the throttler should have a bucket for that topic");

    // Modify store (call store data changed) verify new versions in buckets and subscriptions, old versions are not
    versions = Arrays.asList(new Integer[] { 3, 4 });
    store2 = getDummyStore("store2", versions, 10);
    store2.setCurrentVersion(4);
    quotaEnforcer.handleStoreCreated(store2);
    assertTrue(
        registeredTopics.contains(Version.composeKafkaTopic(store2.getName(), 4)),
        "After adding a store with version " + 4 + ", the throttler should be subscribed to updates for that topic");
    assertTrue(
        quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store2.getName(), 4)),
        "After adding a store with version " + 4 + ", the throttler should have a bucket for that topic");
    assertFalse(
        registeredTopics.contains(Version.composeKafkaTopic(store2.getName(), 2)),
        "After updating a store, the throttler should no longer be subscribed to retired topics");
    assertFalse(
        quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store2.getName(), 2)),
        "After updating a store, the throttler should no longer have a bucket for that topic");

    // Delete a store (call store data deleted) verify nothing in buckets or subscriptions
    quotaEnforcer.handleStoreDeleted(store2.getName());
    for (int v: new Integer[] { 2, 3, 4 }) {
      assertFalse(
          registeredTopics.contains(Version.composeKafkaTopic(store2.getName(), v)),
          "After deleting a store, the throttler should no longer be subscribed to retired topics");
      assertFalse(
          quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store2.getName(), v)),
          "After deleting a store, the throttler should no longer have a bucket for retired topics");
    }

    // Also verify other store is still correct
    assertTrue(
        registeredTopics.contains(Version.composeKafkaTopic(store1.getName(), 1)),
        "After deleting a store, the throttler should still be subscribed to unrelated topics");
    assertTrue(
        quotaEnforcer.listTopics().contains(Version.composeKafkaTopic(store1.getName(), 1)),
        "After deleting a store, the throttler should still have buckets for unrelated topics");
  }

  private Store getDummyStore(String storeName, List<Integer> versions, long rcuQuota) {
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    for (int versionNumber: versions) {
      Version version = new VersionImpl(storeName, versionNumber, Version.composeKafkaTopic(storeName, versionNumber));
      store.addVersion(version);
    }
    store.setReadQuotaInCU(rcuQuota);
    return store;
  }

  public static PartitionAssignment getDummyPartitionAssignment(
      String topic,
      String thisNodeId,
      HelixCustomizedViewOfflinePushRepository customizedViewRepository) {
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(topic).when(partitionAssignment).getTopic();
    Instance thisInstance = new Instance(thisNodeId, "dummyHost", 1234);
    Partition partition = mock(Partition.class);
    doReturn(0).when(partition).getId();
    doReturn(Collections.singletonList(partition)).when(partitionAssignment).getAllPartitions();
    doReturn(Collections.singletonList(thisInstance)).when(partition).getReadyToServeInstances();

    return partitionAssignment;
  }
}
