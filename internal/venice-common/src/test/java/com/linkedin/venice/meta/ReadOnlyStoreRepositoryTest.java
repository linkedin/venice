package com.linkedin.venice.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.utils.Pair;
import java.time.Duration;
import org.testng.annotations.Test;


public class ReadOnlyStoreRepositoryTest {
  @Test
  public void testWaitVersion() {
    Store store = mock(Store.class);

    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(readOnlyStoreRepository).getStore(anyString());
    doCallRealMethod().when(readOnlyStoreRepository).waitVersion(anyString(), anyInt(), any());
    doCallRealMethod().when(readOnlyStoreRepository).waitVersion(anyString(), anyInt(), any(), anyLong());
    doReturn(store).when(readOnlyStoreRepository).refreshOneStore(anyString());

    Pair<Store, Version> res = readOnlyStoreRepository.waitVersion("test", 1, Duration.ofMillis(5000));
    assertNotNull(res);
    assertNotNull(res.getFirst(), "Store should not be null");
    assertEquals(res.getFirst(), store, "Store should be the same");
    assertNull(res.getSecond(), "Version should be null");
    verify(readOnlyStoreRepository).getStore("test");
    verify(readOnlyStoreRepository, atLeast(3)).refreshOneStore("test");
    verify(store, atLeast(3)).getVersion(1);

    Version version = mock(Version.class);
    doReturn(version).when(store).getVersion(1);

    res = readOnlyStoreRepository.waitVersion("test", 1, Duration.ofMillis(5000), 10);
    assertNotNull(res);
    assertNotNull(res.getFirst(), "Store should not be null");
    assertEquals(res.getFirst(), store, "Store should be the same");
    assertNotNull(res.getSecond(), "Version should not be null");
    assertEquals(res.getSecond(), version, "Version should be the same");
  }
}
