package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VersionSplitDiffCoverageTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";

  @Test
  public void testVersionJsonSerializerRoundTripAndTypeGuard() throws Exception {
    VersionJSONSerializer serializer = new VersionJSONSerializer();
    VersionImpl version = new VersionImpl(STORE, 1, "push-1");

    byte[] bytes = serializer.serialize(version, "/test/path");
    Version deserialized = serializer.deserialize(bytes, "/test/path");

    assertTrue(deserialized instanceof VersionImpl);
    assertEquals(deserialized.getStoreName(), STORE);
    assertEquals(deserialized.getNumber(), 1);

    Assert.expectThrows(VeniceException.class, () -> serializer.serialize(Mockito.mock(Version.class), "/test/path"));
  }

  @Test
  public void testVersionAccessorReturnsEmptyWhenVersionsContainerMissing() throws Exception {
    ZkBaseDataAccessor<Version> mockedAccessor = mockDataAccessor();
    HelixVersionAccessor accessor = newAccessorWithInjectedDataAccessor(mockedAccessor);

    String containerPath = accessor.getVersionsContainerPath(STORE);
    when(mockedAccessor.exists(containerPath, AccessOption.PERSISTENT)).thenReturn(false);

    assertTrue(accessor.getVersionsForStore(STORE).isEmpty());
    assertTrue(accessor.getVersionNumbersForStore(STORE).isEmpty());

    verify(mockedAccessor, never()).getChildren(eq(containerPath), any(), eq(AccessOption.PERSISTENT));
    verify(mockedAccessor, never()).getChildNames(eq(containerPath), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testVersionAccessorReadAndWriteBranches() throws Exception {
    ZkBaseDataAccessor<Version> mockedAccessor = mockDataAccessor();
    HelixVersionAccessor accessor = newAccessorWithInjectedDataAccessor(mockedAccessor);

    String containerPath = accessor.getVersionsContainerPath(STORE);
    String versionOnePath = accessor.getVersionZkPath(STORE, 1);
    String versionTwoPath = accessor.getVersionZkPath(STORE, 2);

    Version versionOne = new VersionImpl(STORE, 1, "push-1");
    Version versionTwo = new VersionImpl(STORE, 2, "push-2");

    when(mockedAccessor.exists(containerPath, AccessOption.PERSISTENT)).thenReturn(true);
    when(mockedAccessor.getChildNames(containerPath, AccessOption.PERSISTENT))
        .thenReturn(Collections.singletonList("1"));
    when(mockedAccessor.getChildren(containerPath, null, AccessOption.PERSISTENT))
        .thenReturn(Collections.singletonList(versionOne));

    List<Version> versions = accessor.getVersionsForStore(STORE);
    assertEquals(versions.size(), 1);
    assertEquals(versions.get(0).getNumber(), 1);
    assertEquals(accessor.getVersionNumbersForStore(STORE), Collections.singletonList("1"));

    when(mockedAccessor.exists(versionOnePath, AccessOption.PERSISTENT)).thenReturn(true);
    when(mockedAccessor.exists(versionTwoPath, AccessOption.PERSISTENT)).thenReturn(false);
    when(mockedAccessor.set(eq(versionOnePath), any(Version.class), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockedAccessor.create(eq(versionTwoPath), any(Version.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    accessor.putVersion(STORE, versionOne);
    accessor.putVersion(STORE, versionTwo);

    verify(mockedAccessor).set(eq(versionOnePath), any(Version.class), eq(AccessOption.PERSISTENT));
    verify(mockedAccessor).create(eq(versionTwoPath), any(Version.class), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testVersionAccessorDeleteBranches() throws Exception {
    ZkBaseDataAccessor<Version> mockedAccessor = mockDataAccessor();
    HelixVersionAccessor accessor = newAccessorWithInjectedDataAccessor(mockedAccessor);

    String versionOnePath = accessor.getVersionZkPath(STORE, 1);
    String versionTwoPath = accessor.getVersionZkPath(STORE, 2);
    String containerPath = accessor.getVersionsContainerPath(STORE);

    when(mockedAccessor.exists(versionOnePath, AccessOption.PERSISTENT)).thenReturn(true);
    when(mockedAccessor.exists(versionTwoPath, AccessOption.PERSISTENT)).thenReturn(false);
    when(mockedAccessor.remove(eq(versionOnePath), eq(AccessOption.PERSISTENT))).thenReturn(true);

    accessor.removeVersion(STORE, 1);
    accessor.removeVersion(STORE, 2);

    verify(mockedAccessor).remove(versionOnePath, AccessOption.PERSISTENT);
    verify(mockedAccessor, never()).remove(versionTwoPath, AccessOption.PERSISTENT);

    reset(mockedAccessor);
    when(mockedAccessor.exists(containerPath, AccessOption.PERSISTENT)).thenReturn(false, true);
    when(mockedAccessor.getChildNames(containerPath, AccessOption.PERSISTENT)).thenReturn(Arrays.asList("1", "2"));
    when(mockedAccessor.remove(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);

    accessor.removeAllVersionsForStore(STORE);
    accessor.removeAllVersionsForStore(STORE);

    verify(mockedAccessor).remove(accessor.getVersionZkPath(STORE, 1), AccessOption.PERSISTENT);
    verify(mockedAccessor).remove(accessor.getVersionZkPath(STORE, 2), AccessOption.PERSISTENT);
    verify(mockedAccessor).remove(containerPath, AccessOption.PERSISTENT);
  }

  @Test
  public void testHydrateVersionsFromZkBranchCases() throws Exception {
    ExposedCachedReadOnlyStoreRepository repository = new ExposedCachedReadOnlyStoreRepository();
    HelixVersionAccessor mockedVersionAccessor = Mockito.mock(HelixVersionAccessor.class);
    injectField(CachedReadOnlyStoreRepository.class, repository, "versionAccessor", mockedVersionAccessor);

    repository.hydrateForTest(null);

    Store store = newStore(STORE, newVersion(STORE, 1, "push-1"));
    when(mockedVersionAccessor.hasVersionsContainer(STORE)).thenReturn(false);
    repository.hydrateForTest(store);

    reset(mockedVersionAccessor);
    when(mockedVersionAccessor.hasVersionsContainer(STORE)).thenReturn(true);
    when(mockedVersionAccessor.getVersionsForStore(STORE)).thenReturn(new ArrayList<>(Collections.singletonList(null)));
    repository.hydrateForTest(store);

    reset(mockedVersionAccessor);
    Version persisted = newVersion(STORE, 2, "push-2");
    when(mockedVersionAccessor.hasVersionsContainer(STORE)).thenReturn(true);
    when(mockedVersionAccessor.getVersionsForStore(STORE)).thenReturn(new ArrayList<>(Arrays.asList(persisted, null)));
    repository.hydrateForTest(store);
    assertEquals(store.getVersions().size(), 2);
    assertEquals(store.getVersions().get(0).getNumber(), 1);
    assertEquals(store.getVersions().get(1).getNumber(), 2);

    reset(mockedVersionAccessor);
    Version embeddedVersion = store.getVersions().get(0).cloneVersion();
    Store dedupeStore = newStore(STORE, embeddedVersion);
    when(mockedVersionAccessor.hasVersionsContainer(STORE)).thenReturn(true);
    when(mockedVersionAccessor.getVersionsForStore(STORE))
        .thenReturn(new ArrayList<>(Collections.singletonList(embeddedVersion.cloneVersion())));
    repository.hydrateForTest(dedupeStore);
    assertEquals(dedupeStore.getVersions().size(), 1);

    reset(mockedVersionAccessor);
    Store conflictingStore = newStore(STORE, newVersion(STORE, 1, "push-1"));
    when(mockedVersionAccessor.hasVersionsContainer(STORE)).thenReturn(true);
    when(mockedVersionAccessor.getVersionsForStore(STORE))
        .thenReturn(new ArrayList<>(Collections.singletonList(newVersion(STORE, 1, "push-conflict"))));
    VeniceException exception =
        Assert.expectThrows(VeniceException.class, () -> repository.hydrateForTest(conflictingStore));
    assertTrue(exception.getMessage().contains("conflicting payloads"));
  }

  @Test
  public void testReadOnlyStoreRepositoryOnStoreChangedRefreshesCachedAndUncachedStores() {
    ExposedHelixReadOnlyStoreRepository repository = new ExposedHelixReadOnlyStoreRepository();
    Store store = newStore(STORE, newVersion(STORE, 1, "push-1"));

    repository.onStoreChangedForTest(store);
    assertEquals(repository.refreshCount, 1);
    assertEquals(repository.lastRefreshedStore, STORE);

    repository.storeMap.put(STORE, store);
    repository.onStoreChangedForTest(store);

    assertEquals(repository.refreshCount, 2);
    assertEquals(repository.lastRefreshedStore, STORE);
  }

  @Test
  public void testReadWriteStoreRepositoryLegacyPathAndMetaWriterBranches() throws Exception {
    MetaStoreWriter metaStoreWriter = Mockito.mock(MetaStoreWriter.class);
    HelixReadWriteStoreRepository repository = new HelixReadWriteStoreRepository(
        Mockito.mock(ZkClient.class),
        Mockito.mock(HelixAdapterSerializer.class),
        CLUSTER,
        java.util.Optional.of(metaStoreWriter),
        new ClusterLockManager(CLUSTER),
        false);

    @SuppressWarnings("unchecked")
    ZkBaseDataAccessor<Store> mockedStoreAccessor = Mockito.mock(ZkBaseDataAccessor.class);
    HelixVersionAccessor mockedVersionAccessor = Mockito.mock(HelixVersionAccessor.class);
    injectField(CachedReadOnlyStoreRepository.class, repository, "zkDataAccessor", mockedStoreAccessor);
    injectField(CachedReadOnlyStoreRepository.class, repository, "versionAccessor", mockedVersionAccessor);

    Store store = newStore(STORE, newVersion(STORE, 1, "push-1"));
    store.setStoreMetaSystemStoreEnabled(true);
    when(mockedStoreAccessor.set(anyString(), any(Store.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    Assert.expectThrows(VeniceNoStoreException.class, () -> repository.updateStore(store));

    repository.storeMap.put(STORE, store.cloneStore());
    repository.updateStore(store);

    verify(mockedVersionAccessor).removeAllVersionsForStore(STORE);
    verify(metaStoreWriter).writeStoreProperties(CLUSTER, store);
  }

  @Test
  public void testReadWriteStoreRepositorySplitWriteBranches() throws Exception {
    HelixReadWriteStoreRepository repository = new HelixReadWriteStoreRepository(
        Mockito.mock(ZkClient.class),
        Mockito.mock(HelixAdapterSerializer.class),
        CLUSTER,
        java.util.Optional.empty(),
        new ClusterLockManager(CLUSTER),
        true);

    @SuppressWarnings("unchecked")
    ZkBaseDataAccessor<Store> mockedStoreAccessor = Mockito.mock(ZkBaseDataAccessor.class);
    HelixVersionAccessor mockedVersionAccessor = Mockito.mock(HelixVersionAccessor.class);
    injectField(CachedReadOnlyStoreRepository.class, repository, "zkDataAccessor", mockedStoreAccessor);
    injectField(CachedReadOnlyStoreRepository.class, repository, "versionAccessor", mockedVersionAccessor);

    Version versionOne = newVersion(STORE, 1, "push-1");
    Version versionTwo = newVersion(STORE, 2, "push-2");

    Store priorStore = newStore(STORE, versionOne.cloneVersion());
    Store targetStore = newStore(STORE, versionOne.cloneVersion(), versionTwo.cloneVersion());

    repository.storeMap.put(STORE, targetStore.cloneStore());

    String storePath = repository.getStoreZkPath(STORE);
    when(mockedStoreAccessor.get(eq(storePath), any(), eq(AccessOption.PERSISTENT))).thenReturn(priorStore);
    when(mockedVersionAccessor.getVersionNumbersForStore(STORE)).thenReturn(Arrays.asList("2", "3", "bad-token"));
    when(mockedStoreAccessor.set(anyString(), any(Store.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    repository.updateStore(targetStore);

    verify(mockedVersionAccessor).putVersion(eq(STORE), Mockito.argThat(v -> v.getNumber() == 2));
    verify(mockedVersionAccessor, never()).putVersion(eq(STORE), Mockito.argThat(v -> v.getNumber() == 1));
    verify(mockedVersionAccessor).removeVersion(STORE, 3);
    verify(mockedVersionAccessor, never()).removeVersion(STORE, 2);
    verify(mockedStoreAccessor).set(
        eq(storePath),
        Mockito.argThat(s -> s.getVersions().size() == 1 && s.getVersions().get(0).getNumber() == 1),
        eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testReadWriteStoreRepositorySplitWriteWhenNoPriorStore() throws Exception {
    HelixReadWriteStoreRepository repository = new HelixReadWriteStoreRepository(
        Mockito.mock(ZkClient.class),
        Mockito.mock(HelixAdapterSerializer.class),
        CLUSTER,
        java.util.Optional.empty(),
        new ClusterLockManager(CLUSTER),
        true);

    @SuppressWarnings("unchecked")
    ZkBaseDataAccessor<Store> mockedStoreAccessor = Mockito.mock(ZkBaseDataAccessor.class);
    HelixVersionAccessor mockedVersionAccessor = Mockito.mock(HelixVersionAccessor.class);
    injectField(CachedReadOnlyStoreRepository.class, repository, "zkDataAccessor", mockedStoreAccessor);
    injectField(CachedReadOnlyStoreRepository.class, repository, "versionAccessor", mockedVersionAccessor);

    Version versionOne = newVersion(STORE, 1, "push-1");
    Version versionTwo = newVersion(STORE, 2, "push-2");
    Store targetStore = newStore(STORE, versionOne, versionTwo);

    repository.storeMap.put(STORE, targetStore.cloneStore());

    String storePath = repository.getStoreZkPath(STORE);
    when(mockedStoreAccessor.get(eq(storePath), any(), eq(AccessOption.PERSISTENT))).thenReturn(null);
    when(mockedVersionAccessor.getVersionNumbersForStore(STORE)).thenReturn(Collections.emptyList());
    when(mockedStoreAccessor.set(anyString(), any(Store.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    repository.updateStore(targetStore);

    verify(mockedVersionAccessor).putVersion(eq(STORE), Mockito.argThat(v -> v.getNumber() == 1));
    verify(mockedVersionAccessor).putVersion(eq(STORE), Mockito.argThat(v -> v.getNumber() == 2));
    verify(mockedVersionAccessor, never()).removeVersion(anyString(), Mockito.anyInt());
  }

  @SuppressWarnings("unchecked")
  private static ZkBaseDataAccessor<Version> mockDataAccessor() {
    return Mockito.mock(ZkBaseDataAccessor.class);
  }

  private static HelixVersionAccessor newAccessorWithInjectedDataAccessor(ZkBaseDataAccessor<Version> mockedAccessor)
      throws Exception {
    HelixVersionAccessor accessor =
        new HelixVersionAccessor(Mockito.mock(ZkClient.class), Mockito.mock(HelixAdapterSerializer.class), CLUSTER, 1);
    injectField(HelixVersionAccessor.class, accessor, "versionAccessor", mockedAccessor);
    return accessor;
  }

  private static Version newVersion(String storeName, int number, String pushJobId) {
    return new VersionImpl(storeName, number, pushJobId);
  }

  private static Store newStore(String storeName, Version... versions) {
    Store store = TestUtils.createTestStore(storeName, "owner", 1L);
    store.setVersions(Arrays.asList(versions));
    return store;
  }

  private static void injectField(Class<?> declaringClass, Object target, String fieldName, Object value)
      throws Exception {
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    try {
      field.set(target, value);
    } catch (IllegalAccessException e) {
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      field.set(target, value);
    }
  }

  private static class ExposedCachedReadOnlyStoreRepository extends CachedReadOnlyStoreRepository {
    ExposedCachedReadOnlyStoreRepository() {
      super(
          Mockito.mock(ZkClient.class),
          CLUSTER,
          Mockito.mock(HelixAdapterSerializer.class),
          new ClusterLockManager(CLUSTER));
    }

    void hydrateForTest(Store store) {
      hydrateVersionsFromZk(store);
    }
  }

  private static class ExposedHelixReadOnlyStoreRepository extends HelixReadOnlyStoreRepository {
    private int refreshCount = 0;
    private String lastRefreshedStore;

    ExposedHelixReadOnlyStoreRepository() {
      super(Mockito.mock(ZkClient.class), Mockito.mock(HelixAdapterSerializer.class), CLUSTER);
    }

    @Override
    public Store refreshOneStore(String storeName) {
      refreshCount++;
      lastRefreshedStore = storeName;
      return storeMap.get(storeName);
    }

    void onStoreChangedForTest(Store store) {
      onStoreChanged(store);
    }
  }
}
