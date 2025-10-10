package com.linkedin.davinci.store;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DelegatingStorageEngineTest {
  @Test
  public void testKeyUrnCompressorInitialization() {
    DelegatingStorageEngine delegate = mock(DelegatingStorageEngine.class);
    DelegatingStorageEngine storageEngine = new DelegatingStorageEngine(delegate);
    Assert.assertNotNull(storageEngine.getKeyDictCompressionFunction());
    byte[] key = "abc".getBytes();
    storageEngine.get(0, key);
    verify(delegate, times(1)).get(eq(0), eq(key));

    BytesStreamingCallback bytesStreamingCallback = mock(BytesStreamingCallback.class);
    storageEngine.getByKeyPrefix(0, key, bytesStreamingCallback);
    verify(delegate, times(1)).getByKeyPrefix(eq(0), eq(key), eq(bytesStreamingCallback));
  }
}
