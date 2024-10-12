package com.linkedin.davinci.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.internal.util.collections.Sets;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
    when(mockStore.getVersion(versionNumber)).thenReturn(null).thenReturn(mockVersion);

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
    when(mockStore.getVersion(1)).thenReturn(mockVersion);

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
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitionSet);
    when(mockStorageEngine.getStoreVersionName()).thenReturn(resourceName);
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

  @Test
  public void testCheckWhetherStoragePartitionsShouldBeKeptOrNot() throws NoSuchFieldException, IllegalAccessException {
    StorageService mockStorageService = mock(StorageService.class);
    SafeHelixManager manager = mock(SafeHelixManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    when(manager.getOriginalManager()).thenReturn(helixManager);
    StorageEngineRepository mockStorageEngineRepository = mock(StorageEngineRepository.class);
    AbstractStorageEngine abstractStorageEngine = mock(AbstractStorageEngine.class);
    mockStorageEngineRepository.addLocalStorageEngine(abstractStorageEngine);

    String resourceName = "test_store_v1";
    String storeName = "test_store";

    when(abstractStorageEngine.getStoreVersionName()).thenReturn(resourceName);
    abstractStorageEngine.addStoragePartition(0);
    abstractStorageEngine.addStoragePartition(1);

    String clusterName = "test_cluster";
    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    VeniceClusterConfig mockClusterConfig = mock(VeniceClusterConfig.class);
    when(mockServerConfig.getDataBasePath()).thenReturn("/tmp");
    when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    when(mockVeniceConfigLoader.getVeniceClusterConfig()).thenReturn(mockClusterConfig);
    when(mockVeniceConfigLoader.getVeniceClusterConfig().getClusterName()).thenReturn(clusterName);

    List<AbstractStorageEngine> localStorageEngines = new ArrayList<>();
    localStorageEngines.add(abstractStorageEngine);

    SafeHelixDataAccessor helixDataAccessor = mock(SafeHelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    IdealState idealState = mock(IdealState.class);
    when(helixDataAccessor.getProperty((PropertyKey) any())).thenReturn(idealState);
    Set<String> helixPartitionSet = new HashSet<>(Arrays.asList("test_store_v1_0", "test_store_v1_1"));
    when(idealState.getPartitionSet()).thenReturn(helixPartitionSet);
    ZNRecord record = new ZNRecord("testId");
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> testPartitionZero = new HashMap<>();
    Map<String, String> testPartitionOne = new HashMap<>();
    testPartitionZero.put("lor1-app56585.prod.linkedin.com_1690", "LEADER");
    testPartitionZero.put("lor1-app56614.prod.linkedin.com_1690", "STANDBY");
    testPartitionZero.put("lor1-app110448.prod.linkedin.com_1690", "STANDBY");
    testPartitionOne.put("lor1-app56586.prod.linkedin.com_1690", "LEADER");
    testPartitionOne.put("lor1-app71895.prod.linkedin.com_1690", "STANDBY");
    testPartitionOne.put("lor1-app111181.prod.linkedin.com_1690", "STANDBY");
    mapFields.put("test_store_v1_0", testPartitionZero);
    mapFields.put("test_store_v1_1", testPartitionOne);
    record.setMapFields(mapFields);
    when(idealState.getRecord()).thenReturn(record);
    when(manager.getInstanceName()).thenReturn("lor1-app56586.prod.linkedin.com_1690");

    Set<Integer> partitionSet = new HashSet<>(Arrays.asList(0, 1));
    when(abstractStorageEngine.getPartitionIds()).thenReturn(partitionSet);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        int partitionId = invocation.getArgument(0);
        abstractStorageEngine.getPartitionIds().remove(partitionId);
        return null;
      }
    }).when(abstractStorageEngine).dropPartition(anyInt());

    Field storageEngineRepositoryField = StorageService.class.getDeclaredField("storageEngineRepository");
    storageEngineRepositoryField.setAccessible(true);
    storageEngineRepositoryField.set(mockStorageService, mockStorageEngineRepository);
    when(mockStorageService.getStorageEngineRepository()).thenReturn(mockStorageEngineRepository);
    when(mockStorageService.getStorageEngineRepository().getAllLocalStorageEngines()).thenReturn(localStorageEngines);
    Field configLoaderField = StorageService.class.getDeclaredField("configLoader");
    configLoaderField.setAccessible(true);
    configLoaderField.set(mockStorageService, mockVeniceConfigLoader);
    Field partitionListField = AbstractStorageEngine.class.getDeclaredField("partitionList");
    partitionListField.setAccessible(true);
    partitionListField.set(abstractStorageEngine, abstractStorageEngine.getPartitionList());

    doCallRealMethod().when(mockStorageService).checkWhetherStoragePartitionsShouldBeKeptOrNot(manager);
    mockStorageService.checkWhetherStoragePartitionsShouldBeKeptOrNot(manager);
    Assert.assertFalse(abstractStorageEngine.containsPartition(0));
  }
}
