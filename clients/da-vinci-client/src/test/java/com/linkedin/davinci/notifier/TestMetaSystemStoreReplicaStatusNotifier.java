package com.linkedin.davinci.notifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.system.store.MetaStoreWriter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMetaSystemStoreReplicaStatusNotifier {
  @Test
  public void testNotifierWillNotThrowExceptionWhenStoreIsDeleted() {
    MetaSystemStoreReplicaStatusNotifier notifier = mock(MetaSystemStoreReplicaStatusNotifier.class);
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    when(storeRepository.getStoreOrThrow("store")).thenThrow(VeniceNoStoreException.class);
    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    when(notifier.getMetaStoreWriter()).thenReturn(metaStoreWriter);
    when(notifier.getStoreRepository()).thenReturn(storeRepository);
    doCallRealMethod().when(notifier).report(anyString(), anyInt(), any());
    doCallRealMethod().when(notifier).drop(anyString(), anyInt());
    doCallRealMethod().when(notifier).started(anyString(), anyInt(), anyString());
    // For DROPPED action, no exception will be thrown.
    notifier.drop("store_v1", 1);
    // For other action, VeniceNoStoreException will be thrown.
    Assert.assertThrows(VeniceNoStoreException.class, () -> notifier.started("store_v1", 1, ""));
  }
}
