package com.linkedin.davinci.storage;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.internal.util.collections.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StorageServiceTest {
  private static final String storeName = Utils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
  private static final int versionNumber = 0;
  private static final String storageEngineName = Version.composeKafkaTopic(storeName, versionNumber);

  @Test
  public void testDeleteStorageEngineOnRocksDBError() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(Optional.empty()).thenReturn(Optional.of(mockVersion));

    // no store or no version exists, delete the store
    when(storeRepository.getStoreOrThrow(storeName)).thenThrow(VeniceNoStoreException.class).thenReturn(mockStore);
    StorageEngineFactory factory = mock(StorageEngineFactory.class);
    StorageService.deleteStorageEngineOnRocksDBError(storageEngineName, storeRepository, factory);
    verify(factory, times(1)).removeStorageEngine(storageEngineName);

    StorageService.deleteStorageEngineOnRocksDBError(storageEngineName, storeRepository, factory);
    verify(factory, times(2)).removeStorageEngine(storageEngineName);

    StorageService.deleteStorageEngineOnRocksDBError(storageEngineName, storeRepository, factory);
    verify(factory, times(2)).removeStorageEngine(storageEngineName);
  }

  @Test
  public void testGetStoreAndUserPartitionsMapping() {
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    when(mockServerConfig.getDataBasePath()).thenReturn("/tmp");
    when(configLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);

    AggVersionedStorageEngineStats storageEngineStats = mock(AggVersionedStorageEngineStats.class);
    RocksDBMemoryStats rocksDBMemoryStats = mock(RocksDBMemoryStats.class);
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
        mock(InternalAvroSpecificSerializer.class);
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        mock(InternalAvroSpecificSerializer.class);
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    StorageEngineFactory mockStorageEngineFactory = mock(StorageEngineFactory.class);

    String resourceName = "test_store_v1";
    String storeName = "test_store";
    Store mockStore = mock(Store.class);
    Version mockVersion = mock(Version.class);
    PartitionerConfig mockPartitionerConfig = mock(PartitionerConfig.class);
    when(mockPartitionerConfig.getAmplificationFactor()).thenReturn(1);
    when(mockVersion.getPartitionerConfig()).thenReturn(mockPartitionerConfig);
    when(mockStore.getVersion(1)).thenReturn(Optional.of(mockVersion));

    when(storeRepository.getStore(storeName)).thenReturn(mockStore);

    VeniceStoreVersionConfig storeVersionConfig = mock(VeniceStoreVersionConfig.class);
    when(storeVersionConfig.getStoreVersionName()).thenReturn(resourceName);
    when(storeVersionConfig.isStorePersistenceTypeKnown()).thenReturn(true);
    when(storeVersionConfig.getPersistenceType()).thenReturn(PersistenceType.BLACK_HOLE);
    when(storeVersionConfig.getStorePersistenceType()).thenReturn(PersistenceType.BLACK_HOLE);

    when(configLoader.getStoreConfig(eq(resourceName), eq(PersistenceType.BLACK_HOLE))).thenReturn(storeVersionConfig);

    AbstractStorageEngine mockStorageEngine = mock(AbstractStorageEngine.class);
    when(mockStorageEngineFactory.getStorageEngine(storeVersionConfig, false)).thenReturn(mockStorageEngine);
    Set<Integer> partitionSet = new HashSet<>(Arrays.asList(1, 2, 3));
    when(mockStorageEngine.getPartitionIds()).thenReturn(partitionSet);
    when(mockStorageEngine.getStoreName()).thenReturn(resourceName);
    when(mockStorageEngineFactory.getPersistedStoreNames()).thenReturn(Sets.newSet(resourceName));
    when(mockStorageEngineFactory.getPersistenceType()).thenReturn(PersistenceType.BLACK_HOLE);

    Map<PersistenceType, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap = new HashMap<>();
    persistenceTypeToStorageEngineFactoryMap.put(PersistenceType.BLACK_HOLE, mockStorageEngineFactory);
    StorageService storageService = new StorageService(
        configLoader,
        storageEngineStats,
        rocksDBMemoryStats,
        storeVersionStateSerializer,
        partitionStateSerializer,
        storeRepository,
        true,
        true,
        (s) -> true,
        Optional.of(persistenceTypeToStorageEngineFactoryMap));

    Map<String, Set<Integer>> expectedMapping = new HashMap<>();
    expectedMapping.put(resourceName, partitionSet);
    Assert.assertEquals(storageService.getStoreAndUserPartitionsMapping(), expectedMapping);
  }
}
