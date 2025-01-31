package com.linkedin.davinci.repository;

import static com.linkedin.venice.views.MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
import static com.linkedin.venice.views.VeniceView.VIEW_NAME_SEPARATOR;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlyViewStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryViewAdapterTest {
  @Test
  public void testPassThroughAPIs() {
    NativeMetadataRepository nativeMetadataRepository = mock(NativeMetadataRepository.class);
    NativeMetadataRepositoryViewAdapter repoViewAdapter =
        new NativeMetadataRepositoryViewAdapter(nativeMetadataRepository);
    String storeName = Utils.getUniqueString("testStore");
    String viewName = "testView";
    String viewStoreName = VeniceView.getViewStoreName(storeName, viewName);
    invokeAndVerifyPassThroughAPIs(repoViewAdapter, nativeMetadataRepository, viewStoreName, storeName);
    invokeAndVerifyPassThroughAPIs(repoViewAdapter, nativeMetadataRepository, storeName, storeName);
  }

  @Test
  public void testSubscribeUnsubscribe() throws InterruptedException, ExecutionException, TimeoutException {
    NativeMetadataRepository nativeMetadataRepository = mock(NativeMetadataRepository.class);
    NativeMetadataRepositoryViewAdapter repoViewAdapter =
        new NativeMetadataRepositoryViewAdapter(nativeMetadataRepository);
    String storeName = Utils.getUniqueString("testStore");
    String viewStoreName1 = VeniceView.getViewStoreName(storeName, "testView1");
    String viewStoreName2 = VeniceView.getViewStoreName(storeName, "testView2");
    repoViewAdapter.subscribe(viewStoreName1);
    repoViewAdapter.subscribe(viewStoreName2);
    repoViewAdapter.subscribe(storeName);
    verify(nativeMetadataRepository, times(3)).subscribe(storeName);
    Set<String> subscribedViewStores = repoViewAdapter.getSubscribedViewStores(storeName);
    assertEquals(subscribedViewStores.size(), 2);
    assertTrue(subscribedViewStores.contains(viewStoreName1));
    assertTrue(subscribedViewStores.contains(viewStoreName2));
    repoViewAdapter.unsubscribe(viewStoreName2);
    verify(nativeMetadataRepository, never()).unsubscribe(storeName);
    subscribedViewStores = repoViewAdapter.getSubscribedViewStores(storeName);
    assertEquals(subscribedViewStores.size(), 1);
    assertTrue(subscribedViewStores.contains(viewStoreName1));
    repoViewAdapter.unsubscribe(viewStoreName1);
    verify(nativeMetadataRepository, never()).unsubscribe(storeName);
    assertEquals(repoViewAdapter.getSubscribedViewStores(storeName).size(), 0);
    repoViewAdapter.unsubscribe(storeName);
    verify(nativeMetadataRepository, times(1)).unsubscribe(storeName);
    // We should be able to resubscribe
    repoViewAdapter.subscribe(viewStoreName1);
    verify(nativeMetadataRepository, times(4)).subscribe(storeName);
    assertEquals(repoViewAdapter.getSubscribedViewStores(storeName).iterator().next(), viewStoreName1);

    // Do some simple thread-safe sanity check
    Mockito.clearInvocations(nativeMetadataRepository);
    Runnable subscribeRunnable = () -> {
      try {
        repoViewAdapter.subscribe(viewStoreName1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
    Runnable unSubscribeRunnable = () -> {
      repoViewAdapter.unsubscribe(viewStoreName1);
    };
    CompletableFuture[] completableFutures = new CompletableFuture[50];
    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        completableFutures[i] = CompletableFuture.runAsync(subscribeRunnable);
      } else {
        completableFutures[i] = CompletableFuture.runAsync(unSubscribeRunnable);
      }
    }
    CompletableFuture.allOf(completableFutures).get(1, TimeUnit.SECONDS);
    // Regardless order of events subscribe should be called 25 times
    verify(nativeMetadataRepository, times(25)).subscribe(storeName);
    repoViewAdapter.unsubscribe(viewStoreName1);
    assertEquals(repoViewAdapter.getSubscribedViewStores(storeName).size(), 0);
  }

  @Test
  public void testGetStore() {
    NativeMetadataRepository nativeMetadataRepository = mock(NativeMetadataRepository.class);
    NativeMetadataRepositoryViewAdapter repoViewAdapter =
        new NativeMetadataRepositoryViewAdapter(nativeMetadataRepository);
    String storeName = Utils.getUniqueString("testStore");
    String viewName = "testView";
    String viewStoreName = VeniceView.getViewStoreName(storeName, viewName);
    assertThrows(VeniceNoStoreException.class, () -> repoViewAdapter.getStoreOrThrow(viewStoreName));
    Store store = mock(Store.class);
    List<Version> storeVersions = new ArrayList<>();
    Version storeVersion = new VersionImpl(storeName, 1, "dummyId");
    Version storeVersion2 = new VersionImpl(storeName, 2, "dummyId2");
    storeVersions.add(storeVersion);
    storeVersions.add(storeVersion2);
    // Set some common store version configs
    int storePartitionCount = 6;
    PartitionerConfig storePartitionerConfig = new PartitionerConfigImpl();
    storePartitionerConfig.setPartitionerClass(DefaultVenicePartitioner.class.getCanonicalName());
    for (Version version: storeVersions) {
      version.setPartitionCount(storePartitionCount);
      version.setPartitionerConfig(storePartitionerConfig);
    }
    MaterializedViewParameters.Builder builder = new MaterializedViewParameters.Builder(viewName);
    builder.setPartitionCount(12).setPartitioner(ConstantVenicePartitioner.class.getCanonicalName());
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build());
    Map<String, ViewConfig> storeViewConfigMap = new HashMap<>();
    storeViewConfigMap.put(viewName, viewConfig);
    storeVersion2.setViewConfigs(storeViewConfigMap);
    doReturn(storeVersions).when(store).getVersions();
    doReturn(storeName).when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    doReturn(store).when(nativeMetadataRepository).getStore(storeName);
    assertEquals(repoViewAdapter.getStoreOrThrow(storeName).getName(), storeName);
    // Now get the view store and verify its properties
    Store viewStore = repoViewAdapter.getStoreOrThrow(viewStoreName);
    assertTrue(viewStore instanceof ReadOnlyViewStore);
    assertEquals(viewStore.getName(), viewStoreName);
    // v1 should be filtered since it doesn't have view configs
    assertNull(viewStore.getVersion(1));
    Version viewStoreVersion = viewStore.getVersion(viewStore.getCurrentVersion());
    assertNotNull(viewStoreVersion);
    assertTrue(viewStoreVersion instanceof ReadOnlyViewStore.ReadOnlyMaterializedViewVersion);
    assertEquals(viewStoreVersion.getPartitionCount(), 12);
    assertEquals(
        viewStoreVersion.getPartitionerConfig().getPartitionerClass(),
        ConstantVenicePartitioner.class.getCanonicalName());
    String viewStoreVersionTopicName =
        Version.composeKafkaTopic(storeName, 2) + VIEW_NAME_SEPARATOR + viewName + MATERIALIZED_VIEW_TOPIC_SUFFIX;
    assertEquals(viewStoreVersion.kafkaTopicName(), viewStoreVersionTopicName);
  }

  @Test
  public void testStoreDataChangedListener() throws InterruptedException {
    AtomicInteger storeChangeCounter = new AtomicInteger(0);
    AtomicInteger viewStoreChangeCounter = new AtomicInteger(0);
    StoreDataChangedListener testDataChangedListener = new StoreDataChangedListener() {
      @Override
      public void handleStoreChanged(Store store) {
        if (VeniceView.isViewStore(store.getName())) {
          viewStoreChangeCounter.incrementAndGet();
        } else {
          storeChangeCounter.incrementAndGet();
        }
      }
    };
    NativeMetadataRepository nativeMetadataRepository = mock(NativeMetadataRepository.class);
    NativeMetadataRepositoryViewAdapter repoViewAdapter =
        new NativeMetadataRepositoryViewAdapter(nativeMetadataRepository);
    String storeName = Utils.getUniqueString("testStore");
    String viewStoreName1 = VeniceView.getViewStoreName(storeName, "testView1");
    String viewStoreName2 = VeniceView.getViewStoreName(storeName, "testView2");
    repoViewAdapter.subscribe(viewStoreName1);
    repoViewAdapter.subscribe(viewStoreName2);
    assertEquals(repoViewAdapter.getSubscribedViewStores(storeName).size(), 2);
    repoViewAdapter.registerStoreDataChangedListener(testDataChangedListener);
    ArgumentCaptor<StoreDataChangedListener> wrappedListenerCaptor =
        ArgumentCaptor.forClass(StoreDataChangedListener.class);
    verify(nativeMetadataRepository, times(1)).registerStoreDataChangedListener(wrappedListenerCaptor.capture());
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(Collections.emptyList()).when(store).getVersions();
    StoreDataChangedListener listenerViewAdapter = wrappedListenerCaptor.getValue();
    listenerViewAdapter.handleStoreCreated(store);
    listenerViewAdapter.handleStoreChanged(store);
    listenerViewAdapter.handleStoreChanged(store);
    listenerViewAdapter.handleStoreDeleted(store);
    assertEquals(storeChangeCounter.get(), 2);
    assertEquals(viewStoreChangeCounter.get(), 4);
    repoViewAdapter.unregisterStoreDataChangedListener(testDataChangedListener);
    verify(nativeMetadataRepository, times(1)).unregisterStoreDataChangedListener(wrappedListenerCaptor.capture());
    // Ensure the storeDataChangedAdapterMap is working and we are unregistering the correct listener
    assertEquals(wrappedListenerCaptor.getValue(), listenerViewAdapter);
    // We should be able to re-register the same listener
    repoViewAdapter.registerStoreDataChangedListener(testDataChangedListener);
    verify(nativeMetadataRepository, times(2)).registerStoreDataChangedListener(wrappedListenerCaptor.capture());
  }

  private void invokeAndVerifyPassThroughAPIs(
      NativeMetadataRepositoryViewAdapter adapter,
      NativeMetadataRepository repo,
      String inputStoreName,
      String expectedRepoInvocationStoreName) {
    Mockito.clearInvocations(repo);
    adapter.refresh();
    verify(repo, times(1)).refresh();
    adapter.clear();
    verify(repo, times(1)).clear();
    adapter.getVeniceCluster(inputStoreName);
    verify(repo, times(1)).getVeniceCluster(expectedRepoInvocationStoreName);
    adapter.getKeySchema(inputStoreName);
    verify(repo, times(1)).getKeySchema(expectedRepoInvocationStoreName);
    adapter.getValueSchema(inputStoreName, 1);
    verify(repo, times(1)).getValueSchema(expectedRepoInvocationStoreName, 1);
    adapter.hasValueSchema(inputStoreName, 1);
    verify(repo, times(1)).hasValueSchema(expectedRepoInvocationStoreName, 1);
    adapter.getValueSchemaId(inputStoreName, "");
    verify(repo, times(1)).getValueSchemaId(expectedRepoInvocationStoreName, "");
    adapter.getValueSchemas(inputStoreName);
    verify(repo, times(1)).getValueSchemas(expectedRepoInvocationStoreName);
    adapter.getSupersetOrLatestValueSchema(inputStoreName);
    verify(repo, times(1)).getSupersetOrLatestValueSchema(expectedRepoInvocationStoreName);
    adapter.getSupersetSchema(inputStoreName);
    verify(repo, times(1)).getSupersetSchema(expectedRepoInvocationStoreName);
    adapter.getDerivedSchemaId(inputStoreName, "");
    verify(repo, times(1)).getDerivedSchemaId(expectedRepoInvocationStoreName, "");
    adapter.getDerivedSchema(inputStoreName, 1, 1);
    verify(repo, times(1)).getDerivedSchema(expectedRepoInvocationStoreName, 1, 1);
    adapter.getDerivedSchemas(inputStoreName);
    verify(repo, times(1)).getDerivedSchemas(expectedRepoInvocationStoreName);
    adapter.getLatestDerivedSchema(inputStoreName, 1);
    verify(repo, times(1)).getLatestDerivedSchema(expectedRepoInvocationStoreName, 1);
    adapter.getReplicationMetadataSchema(inputStoreName, 1, 1);
    verify(repo, times(1)).getReplicationMetadataSchema(expectedRepoInvocationStoreName, 1, 1);
    adapter.getReplicationMetadataSchemas(inputStoreName);
    verify(repo, times(1)).getReplicationMetadataSchemas(expectedRepoInvocationStoreName);
    adapter.hasStore(inputStoreName);
    verify(repo, times(1)).hasStore(expectedRepoInvocationStoreName);
    adapter.refreshOneStore(inputStoreName);
    verify(repo, times(1)).refreshOneStore(expectedRepoInvocationStoreName);
    adapter.getAllStores();
    verify(repo, times(1)).getAllStores();
    adapter.getTotalStoreReadQuota();
    verify(repo, times(1)).getTotalStoreReadQuota();
    adapter.getBatchGetLimit(inputStoreName);
    verify(repo, times(1)).getBatchGetLimit(expectedRepoInvocationStoreName);
    adapter.isReadComputationEnabled(inputStoreName);
    verify(repo, times(1)).isReadComputationEnabled(expectedRepoInvocationStoreName);
  }
}
