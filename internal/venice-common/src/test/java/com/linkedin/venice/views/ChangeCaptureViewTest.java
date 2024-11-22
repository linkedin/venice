package com.linkedin.venice.views;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
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
    String storeName = "test-store";
    Store NonChunkedStore = Mockito.mock(Store.class);
    Mockito.when(NonChunkedStore.isChunkingEnabled()).thenReturn(false);
    Store NonAAStore = Mockito.mock(Store.class);
    Mockito.when(NonAAStore.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store AAChunkedStore = Mockito.mock(Store.class);
    Mockito.when(AAChunkedStore.isChunkingEnabled()).thenReturn(true);
    Mockito.when(AAChunkedStore.isActiveActiveReplicationEnabled()).thenReturn(true);

    Assert.assertThrows(() -> new ChangeCaptureView(props, storeName, viewParams).validateConfigs(NonChunkedStore));
    Assert.assertThrows(() -> new ChangeCaptureView(props, storeName, viewParams).validateConfigs(NonAAStore));
    // Should now throw
    new ChangeCaptureView(props, storeName, viewParams).validateConfigs(AAChunkedStore);
  }

  @Test
  public void testCCViewTopicProcessing() {
    String storeName = "test-store";
    int version = 3;
    Store testStore = Mockito.mock(Store.class);
    Mockito.when(testStore.getName()).thenReturn(storeName);
    ChangeCaptureView changeCaptureView = new ChangeCaptureView(new Properties(), storeName, new HashMap<>());
    Map<String, VeniceProperties> changeCaptureViewTopicMap =
        changeCaptureView.getTopicNamesAndConfigsForVersion(version);
    assertEquals(changeCaptureViewTopicMap.size(), 1);
    for (Map.Entry<String, VeniceProperties> entry: changeCaptureViewTopicMap.entrySet()) {
      String viewTopic = entry.getKey();
      assertTrue(VeniceView.isViewTopic(viewTopic));
      assertEquals(VeniceView.parseStoreFromViewTopic(viewTopic), storeName);
      assertEquals(VeniceView.parseVersionFromViewTopic(viewTopic), version);
    }
  }
}
