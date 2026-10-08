package com.linkedin.venice.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import java.time.Duration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ReadOnlyStoreRepositoryTest {
  @DataProvider
  public Object[][] encryptionKeys() {
    return new Object[][] { { "urn:test:key:1" }, { "" }, { null } };
  }

  @Test(dataProvider = "encryptionKeys")
  public void testEncryptionKeyLookupUsesLocalMetadata(String keyUrn) {
    Store store = mock(Store.class);
    doReturn(keyUrn).when(store).getPubSubEncryptionKeyUrn();
    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class, CALLS_REAL_METHODS);
    doReturn(store).when(repository).getStoreOrThrow("existing");
    doThrow(new VeniceNoStoreException("missing")).when(repository).getStoreOrThrow("missing");

    assertEquals(repository.getPubSubEncryptionKeyUrn("existing"), keyUrn);
    assertNull(repository.getPubSubEncryptionKeyUrn("missing"));
    if (keyUrn == null || keyUrn.isEmpty()) {
      doReturn("urn:test:key:1").when(store).getPubSubEncryptionKeyUrn();
      assertEquals(repository.getPubSubEncryptionKeyUrn("existing"), "urn:test:key:1");
    }
    Store lateStore = mock(Store.class);
    doReturn("urn:test:key:2").when(lateStore).getPubSubEncryptionKeyUrn();
    doReturn(lateStore).when(repository).getStoreOrThrow("missing");
    assertEquals(repository.getPubSubEncryptionKeyUrn("missing"), "urn:test:key:2");
    verify(repository, never()).getStore(anyString());
    verify(repository, never()).refreshOneStore(anyString());
  }

  @Test
  public void testEncryptionKeyLookupPropagatesRepositoryFailure() {
    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class, CALLS_REAL_METHODS);
    IllegalStateException failure = new IllegalStateException("repository unavailable");
    doThrow(failure).when(repository).getStoreOrThrow("store");

    assertSame(expectThrows(IllegalStateException.class, () -> repository.getPubSubEncryptionKeyUrn("store")), failure);
  }

  @Test
  public void testWaitVersion() {
    Store store = mock(Store.class);

    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(readOnlyStoreRepository).getStore(anyString());
    doCallRealMethod().when(readOnlyStoreRepository).waitVersion(anyString(), anyInt(), any());
    doCallRealMethod().when(readOnlyStoreRepository).waitVersion(anyString(), anyInt(), any(), anyLong());
    doReturn(store).when(readOnlyStoreRepository).refreshOneStore(anyString());

    StoreVersionInfo res = readOnlyStoreRepository.waitVersion("test", 1, Duration.ofMillis(5000));
    assertNotNull(res);
    assertNotNull(res.getStore(), "Store should not be null");
    assertEquals(res.getStore(), store, "Store should be the same");
    assertNull(res.getVersion(), "Version should be null");
    verify(readOnlyStoreRepository).getStore("test");
    verify(readOnlyStoreRepository, atLeast(3)).refreshOneStore("test");
    verify(store, atLeast(3)).getVersion(1);

    Version version = mock(Version.class);
    doReturn(version).when(store).getVersion(1);

    res = readOnlyStoreRepository.waitVersion("test", 1, Duration.ofMillis(5000), 10);
    assertNotNull(res);
    assertNotNull(res.getStore(), "Store should not be null");
    assertEquals(res.getStore(), store, "Store should be the same");
    assertNotNull(res.getVersion(), "Version should not be null");
    assertEquals(res.getVersion(), version, "Version should be the same");
  }
}
