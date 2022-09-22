package com.linkedin.venice.cleaner;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;


public class LeakedResourceCleanerTest {
  private static class StorageEngineMockConfig {
    public final int version;
    public final boolean existingInZK;
    public final boolean hasIngestionTask;

    public StorageEngineMockConfig(int version, boolean existingInZK, boolean hasIngestionTask) {
      this.version = version;
      this.existingInZK = existingInZK;
      this.hasIngestionTask = hasIngestionTask;
    }
  }

  private List<AbstractStorageEngine> constructStorageEngineForStore(
      ReadOnlyStoreRepository storeRepository, // mocked ReadOnlyStoreRepository
      StoreIngestionService ingestionService, // mocked StoreIngestionService
      String storeName,
      boolean returnStore,
      StorageEngineMockConfig... configs) {
    List<AbstractStorageEngine> mockedEngines = new ArrayList<>();
    Store store = mock(Store.class);
    if (returnStore) {
      doReturn(store).when(storeRepository).getStoreOrThrow(storeName);
    } else {
      doThrow(new VeniceNoStoreException(storeName + " does not exist")).when(storeRepository)
          .getStoreOrThrow(storeName);
    }
    List<Version> versions = new ArrayList<>();
    for (StorageEngineMockConfig config: configs) {
      AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
      String topic = Version.composeKafkaTopic(storeName, config.version);
      doReturn(topic).when(storageEngine).getStoreName();
      if (config.existingInZK) {
        Version version = mock(Version.class);
        doReturn(config.version).when(version).getNumber();
        versions.add(version);
        doReturn(Optional.of(version)).when(store).getVersion(config.version);
      }
      doReturn(config.hasIngestionTask).when(ingestionService).containsRunningConsumption(topic);
      mockedEngines.add(storageEngine);
    }
    doReturn(versions).when(store).getVersions();

    return mockedEngines;
  }

  @Test
  public void testCleanupLeakedResources() throws Exception {
    String storeNameWithNoVersionInZK = Utils.getUniqueString("store_with_no_version_in_zk");
    String storeNameWithoutLeakedResource = Utils.getUniqueString("store_without_leaked_resource");
    String storeNameWithLeakedResource = Utils.getUniqueString("store_with_leaked_resource");
    String nonExistingStoreName = Utils.getUniqueString("store_non_existent");

    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    StorageService storageService = mock(StorageService.class);
    StoreIngestionService ingestionService = mock(StoreIngestionService.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    List<AbstractStorageEngine> storageEngineList = new ArrayList<>();
    /**
     * Store with no version in ZK contains one version on disk, and no running ingestion task.
     */
    storageEngineList.addAll(
        constructStorageEngineForStore(
            storeRepository,
            ingestionService,
            storeNameWithNoVersionInZK,
            true,
            new StorageEngineMockConfig(1, false, false)));
    /**
     * Store without leaked resources.
     */
    storageEngineList.addAll(
        constructStorageEngineForStore(
            storeRepository,
            ingestionService,
            storeNameWithoutLeakedResource,
            true,
            new StorageEngineMockConfig(1, true, true),
            new StorageEngineMockConfig(2, true, true)));
    /**
     * Store with leaked resources.
     */
    storageEngineList.addAll(
        constructStorageEngineForStore(
            storeRepository,
            ingestionService,
            storeNameWithLeakedResource,
            true,
            new StorageEngineMockConfig(1, false, true),
            new StorageEngineMockConfig(2, false, false),
            new StorageEngineMockConfig(3, true, true),
            new StorageEngineMockConfig(4, true, true),
            new StorageEngineMockConfig(5, false, false)));
    /**
     * Non-existing Store
     */
    storageEngineList.addAll(
        constructStorageEngineForStore(
            storeRepository,
            ingestionService,
            nonExistingStoreName,
            false,
            new StorageEngineMockConfig(1, false, false)));

    doReturn(storageEngineList).when(storageEngineRepository).getAllLocalStorageEngines();

    LeakedResourceCleaner cleaner = new LeakedResourceCleaner(
        storageEngineRepository,
        1000,
        storeRepository,
        ingestionService,
        storageService,
        new MetricsRepository());
    cleaner.setNonExistentStoreCleanupInterval(1 * Time.MS_PER_SECOND);
    cleaner.start();

    verify(storageService, timeout(10 * 1000))
        .removeStorageEngine(Version.composeKafkaTopic(storeNameWithLeakedResource, 5));
    verify(storageService).removeStorageEngine(Version.composeKafkaTopic(storeNameWithLeakedResource, 2));
    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithLeakedResource, 1));
    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithLeakedResource, 3));
    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithLeakedResource, 4));

    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithoutLeakedResource, 1));
    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithoutLeakedResource, 2));

    verify(storageService, never()).removeStorageEngine(Version.composeKafkaTopic(storeNameWithNoVersionInZK, 1));
    verify(storageService, timeout(10 * Time.MS_PER_SECOND))
        .removeStorageEngine(Version.composeKafkaTopic(nonExistingStoreName, 1));

    cleaner.stop();
  }
}
