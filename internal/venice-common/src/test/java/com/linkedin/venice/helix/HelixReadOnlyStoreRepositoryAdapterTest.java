package com.linkedin.venice.helix;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.fail;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.system.store.MetaStoreWriter;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HelixReadOnlyStoreRepositoryAdapterTest {
  @DataProvider
  public Object[][] systemStoreTypes() {
    return new Object[][] { { VeniceSystemStoreType.META_STORE }, { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE } };
  }

  @Test(dataProvider = "systemStoreTypes")
  public void testEncryptionKeyLookupDoesNotRefresh(VeniceSystemStoreType systemStoreType) {
    ReadOnlyStoreRepository regularRepository = mock(ReadOnlyStoreRepository.class);
    HelixReadOnlyZKSharedSystemStoreRepository sharedRepository =
        mock(HelixReadOnlyZKSharedSystemStoreRepository.class);
    HelixReadOnlyStoreRepositoryAdapter adapter =
        new HelixReadOnlyStoreRepositoryAdapter(sharedRepository, regularRepository, "test-cluster");
    String keyUrn = "urn:test:key:1";
    doReturn(keyUrn).when(regularRepository).getPubSubEncryptionKeyUrn("store");
    assertEquals(adapter.getPubSubEncryptionKeyUrn("store"), keyUrn);
    assertNull(adapter.getPubSubEncryptionKeyUrn("missing"));

    String systemStoreName = systemStoreType.getSystemStoreName("store");
    String sharedStoreName = systemStoreType.getZkSharedStoreName();
    assertNull(adapter.getPubSubEncryptionKeyUrn(systemStoreName));
    verify(sharedRepository, never()).getPubSubEncryptionKeyUrn(anyString());

    doReturn(true).when(regularRepository).hasStore("store");
    assertNull(adapter.getPubSubEncryptionKeyUrn(systemStoreName));
    doReturn("urn:test:key:shared").when(sharedRepository).getPubSubEncryptionKeyUrn(sharedStoreName);
    assertEquals(adapter.getPubSubEncryptionKeyUrn(systemStoreName), "urn:test:key:shared");

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
