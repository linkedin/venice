package com.linkedin.davinci.storage;

import static org.mockito.Mockito.*;

import com.linkedin.davinci.store.StorageEngineFactory;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
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
}
