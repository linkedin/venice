package com.linkedin.venice.helix;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HelixReadOnlyStoreRepositoryAdapterTest {
  @DataProvider
  public Object[][] cacheMisses() {
    return new Object[][] { { null, true }, { VeniceSystemStoreType.META_STORE, true },
        { VeniceSystemStoreType.META_STORE, false }, { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, true },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, false } };
  }

  @Test(dataProvider = "cacheMisses", timeOut = 10_000)
  public void testCacheLookupDoesNotWaitForRefresh(VeniceSystemStoreType type, boolean regularMissing)
      throws Exception {
    HelixReadOnlyStoreRepository regular =
        spy(new HelixReadOnlyStoreRepository(mock(ZkClient.class), new HelixAdapterSerializer(), "test-cluster"));
    HelixReadOnlyZKSharedSystemStoreRepository shared = spy(
        new HelixReadOnlyZKSharedSystemStoreRepository(
            mock(ZkClient.class),
            new HelixAdapterSerializer(),
            "system-cluster"));
    Store regularStore = TestUtils.createTestStore("store", "owner", 1L);
    Store sharedStore = TestUtils
        .createTestStore((type == null ? VeniceSystemStoreType.META_STORE : type).getZkSharedStoreName(), "owner", 1L);
    Store visibleStore = type == null ? regularStore : new SystemStore(sharedStore, type, regularStore);
    visibleStore.addVersion(new VersionImpl(visibleStore.getName(), 1, "test-push"));
    if (!regularMissing) {
      regular.storeMap.put(regularStore.getName(), regularStore);
    }
    if (regularMissing) {
      shared.storeMap.put(sharedStore.getName(), sharedStore);
    }
    doReturn(regularStore).when(regular).getStoreFromZk(regularStore.getName());
    doReturn(sharedStore).when(shared).getStoreFromZk(sharedStore.getName());
    CachedReadOnlyStoreRepository missing = regularMissing ? regular : shared;
    Store missingStore = regularMissing ? regularStore : sharedStore;
    CountDownLatch refreshStarted = new CountDownLatch(1);
    CountDownLatch releaseRefresh = new CountDownLatch(1);
    doAnswer(invocation -> {
      refreshStarted.countDown();
      releaseRefresh.await();
      return missingStore;
    }).when(missing).getStoreFromZk(missingStore.getName());
    HelixReadOnlyStoreRepositoryAdapter adapter =
        new HelixReadOnlyStoreRepositoryAdapter(shared, regular, "test-cluster");
    FutureTask<Store> refresh = new FutureTask<>(() -> adapter.refreshOneStore(visibleStore.getName()));
    new Thread(refresh).start();
    try {
      assertTrue(refreshStarted.await(3, TimeUnit.SECONDS));
      assertNull(
          CompletableFuture.supplyAsync(() -> adapter.getStoreFromCache(visibleStore.getName()))
              .get(1, TimeUnit.SECONDS));
      verify(missing).getStoreFromZk(missingStore.getName());
      missing.storeMap.put(missingStore.getName(), missingStore);
      assertEquals(
          CompletableFuture.supplyAsync(() -> adapter.getStoreFromCache(visibleStore.getName()))
              .get(1, TimeUnit.SECONDS)
              .getVersion(1)
              .getNumber(),
          1);
      assertEquals(releaseRefresh.getCount(), 1L);
    } finally {
      releaseRefresh.countDown();
      refresh.get(3, TimeUnit.SECONDS);
    }
  }

  @DataProvider
  public Object[][] systemStoreNames() {
    return new Object[][] { { VeniceSystemStoreType.META_STORE.getSystemStoreName("store") },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("store") },
        { VeniceSystemStoreType.META_STORE.getZkSharedStoreName() },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName() },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix() },
        { VeniceSystemStoreUtils.getParticipantStoreNameForCluster("test-cluster") },
        { VeniceSystemStoreUtils.getPushJobDetailsStoreName() },
        { VeniceSystemStoreUtils.getParentControllerMetadataStoreNameForCluster("test-cluster") },
        { String.format(Store.SYSTEM_STORE_FORMAT, "other") } };
  }

  @Test(dataProvider = "systemStoreNames")
  public void testEncryptionKeyLookupSkipsSystemStores(String systemStoreName) {
    ReadOnlyStoreRepository regularRepository = mock(ReadOnlyStoreRepository.class);
    HelixReadOnlyZKSharedSystemStoreRepository sharedRepository =
        mock(HelixReadOnlyZKSharedSystemStoreRepository.class);
    HelixReadOnlyStoreRepositoryAdapter adapter =
        new HelixReadOnlyStoreRepositoryAdapter(sharedRepository, regularRepository, "test-cluster");
    String keyUrn = "urn:test:key:1";
    doReturn(keyUrn).when(regularRepository).getPubSubEncryptionKeyUrn("store");
    assertEquals(adapter.getPubSubEncryptionKeyUrn("store"), keyUrn);
    assertNull(adapter.getPubSubEncryptionKeyUrn("missing"));

    doReturn(true).when(regularRepository).hasStore("store");
    doReturn(keyUrn).when(regularRepository).getPubSubEncryptionKeyUrn(systemStoreName);
    doReturn(keyUrn).when(sharedRepository).getPubSubEncryptionKeyUrn(anyString());
    assertNull(adapter.getPubSubEncryptionKeyUrn(systemStoreName));

    verify(regularRepository, never()).getPubSubEncryptionKeyUrn(systemStoreName);
    verify(regularRepository, never()).hasStore(anyString());
    verify(sharedRepository, never()).getPubSubEncryptionKeyUrn(anyString());
    verify(regularRepository, never()).getStore(anyString());
    verify(sharedRepository, never()).getStore(anyString());
    verify(regularRepository, never()).refreshOneStore(anyString());
    verify(sharedRepository, never()).refreshOneStore(anyString());
  }

  @Test
  public void testStoreDeleteHandler() {
    HelixReadOnlyStoreRepositoryAdapter adapter = mock(HelixReadOnlyStoreRepositoryAdapter.class);
    Set<StoreDataChangedListener> dataChangedListenerSet = new HashSet<>();
    StoreDataChangedListener dataChangedListener = mock(StoreDataChangedListener.class);
    dataChangedListenerSet.add(dataChangedListener);
    when(adapter.getListeners()).thenReturn(dataChangedListenerSet);
    HelixReadOnlyStoreRepositoryAdapter.VeniceStoreDataChangedListener listener =
        adapter.new VeniceStoreDataChangedListener();
    Store store = mock(Store.class);
    when(store.getName()).thenReturn("abc");
    try {
      listener.handleStoreDeleted(store);
    } catch (Exception e) {
      fail("Should be able to handle delete store");
    }
  }

  @Test
  public void testRemoveValueSchema() {
    String storeName = "abc";
    HelixReadOnlyZKSharedSchemaRepository readOnlyZKSharedSchemaRepository =
        mock(HelixReadOnlyZKSharedSchemaRepository.class);
    ReadWriteSchemaRepository repository = mock(ReadWriteSchemaRepository.class);
    HelixReadWriteSchemaRepositoryAdapter adapter =
        new HelixReadWriteSchemaRepositoryAdapter(readOnlyZKSharedSchemaRepository, repository);
    adapter.removeValueSchema(storeName, 2);
    adapter.removeDerivedSchema(storeName, 2, 1);
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    HelixSchemaAccessor schemaAccessor = mock(HelixSchemaAccessor.class);
    HelixReadWriteSchemaRepository readWriteSchemaRepository =
        new HelixReadWriteSchemaRepository(storeRepository, Optional.of(metaStoreWriter), schemaAccessor);
    doReturn(true).when(storeRepository).hasStore(anyString());
    Store store = mock(Store.class);
    doReturn(store).when(storeRepository).getStore(anyString());
    doReturn(store).when(storeRepository).getStoreOrThrow(anyString());
    doReturn(1).when(store).getLatestSuperSetValueSchemaId();
    SchemaEntry schemaEntry = mock(SchemaEntry.class);
    doReturn(1).when(schemaEntry).getId();
    doReturn(schemaEntry).when(schemaAccessor).getValueSchema(anyString(), anyString());
    readWriteSchemaRepository.removeValueSchema(storeName, 1);

    verify(repository, times(1)).removeValueSchema(storeName, 2);

    readWriteSchemaRepository.removeValueSchema(storeName, 2);

  }

}
