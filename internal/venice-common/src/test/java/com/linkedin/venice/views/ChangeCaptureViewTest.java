package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChangeCaptureViewTest {
  @Test
  public void testValidateConfigs() {
    Properties props = new Properties();
    Map<String, String> viewParams = new HashMap<>();
    Store NonChunkedStore = Mockito.mock(Store.class);
    Mockito.when(NonChunkedStore.isChunkingEnabled()).thenReturn(false);
    Store NonAAStore = Mockito.mock(Store.class);
    Mockito.when(NonAAStore.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store AAChunkedStore = Mockito.mock(Store.class);
    Mockito.when(AAChunkedStore.isChunkingEnabled()).thenReturn(true);
    Mockito.when(AAChunkedStore.isActiveActiveReplicationEnabled()).thenReturn(true);

    Assert.assertThrows(() -> new ChangeCaptureView(props, NonChunkedStore, viewParams).validateConfigs());
    Assert.assertThrows(() -> new ChangeCaptureView(props, NonAAStore, viewParams).validateConfigs());
    // Should now throw
    new ChangeCaptureView(props, AAChunkedStore, viewParams).validateConfigs();
  }
}
