package com.linkedin.venice.helix;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;


public class HelixReadOnlyStoreRepositoryAdapterTest {
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

}
