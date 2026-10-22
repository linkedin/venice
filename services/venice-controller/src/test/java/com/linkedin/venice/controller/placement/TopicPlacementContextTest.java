package com.linkedin.venice.controller.placement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.placement.TopicPlacementContext.MigrationCategory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TopicPlacementContextTest {
  private TopicPlacementContext context(Store store) {
    return new TopicPlacementContext("cluster", "store", "store_v1", "destination", true, store);
  }

  private Store store(boolean hybrid, String viewClass, boolean pushStatus, boolean heartbeat, boolean blob) {
    Store store = mock(Store.class);
    when(store.isHybrid()).thenReturn(hybrid);
    when(store.isDaVinciPushStatusStoreEnabled()).thenReturn(pushStatus);
    when(store.getIsDavinciHeartbeatReported()).thenReturn(heartbeat);
    when(store.isBlobTransferEnabled()).thenReturn(blob);
    Map<String, ViewConfig> views = new HashMap<>();
    if (viewClass != null) {
      views.put("view", new ViewConfigImpl(viewClass, Collections.emptyMap()));
    }
    when(store.getViewConfigs()).thenReturn(views);
    return store;
  }

  @DataProvider
  public Object[][] categories() {
    String cdc = "com.linkedin.venice.views.ChangeCaptureView";
    return new Object[][] { { false, null, false, false, false, MigrationCategory.BATCH_VT },
        { true, null, false, false, false, MigrationCategory.HYBRID_VT },
        { false, cdc, false, false, false, MigrationCategory.CDC_VT },
        { false, null, true, false, false, MigrationCategory.DAVINCI_VT },
        { false, null, false, true, false, MigrationCategory.DAVINCI_VT },
        { false, null, false, false, true, MigrationCategory.DAVINCI_VT },
        { false, null, true, true, true, MigrationCategory.DAVINCI_VT },
        { true, cdc, true, true, true, MigrationCategory.CDC_VT },
        { true, cdc, false, false, false, MigrationCategory.CDC_VT },
        { true, null, true, false, false, MigrationCategory.DAVINCI_VT },
        { true, null, false, true, false, MigrationCategory.DAVINCI_VT },
        { true, null, false, false, true, MigrationCategory.DAVINCI_VT },
        { false, cdc, true, true, true, MigrationCategory.CDC_VT },
        { false, "com.linkedin.venice.views.MaterializedView", false, false, false, MigrationCategory.BATCH_VT },
        { true, "com.linkedin.venice.views.MaterializedView", false, false, false, MigrationCategory.HYBRID_VT },
        { false, "example.ChangeCaptureView", false, false, false, MigrationCategory.BATCH_VT } };
  }

  @Test(dataProvider = "categories")
  public void testMetadataClassification(
      boolean hybrid,
      String viewClass,
      boolean pushStatus,
      boolean heartbeat,
      boolean blob,
      MigrationCategory expected) {
    assertEquals(context(store(hybrid, viewClass, pushStatus, heartbeat, blob)).getMigrationCategory(), expected);
  }

  @Test
  public void testCompatibilityConstructorIsUnverified() {
    TopicPlacementContext context = new TopicPlacementContext("cluster", "store", "store_v1", null, true);
    assertEquals(context.getMigrationCategory(), MigrationCategory.UNKNOWN);
    assertEquals(context(null).getMigrationCategory(), MigrationCategory.UNKNOWN);
  }

  @DataProvider
  public Object[][] malformedViews() {
    ViewConfig missingClass = new ViewConfigImpl(null, Collections.emptyMap());
    ViewConfig blankClass = new ViewConfigImpl(" ", Collections.emptyMap());
    return new Object[][] { { Collections.singletonMap("view", null) },
        { Collections.singletonMap("view", missingClass) }, { Collections.singletonMap("view", blankClass) },
        { Collections.singletonMap("view", new ViewConfigImpl(null)) } };
  }

  @Test(dataProvider = "malformedViews")
  public void testMalformedViewMetadataIsUnverified(Map<String, ViewConfig> views) {
    for (boolean hybrid: new boolean[] { false, true }) {
      Store store = store(hybrid, null, false, false, false);
      when(store.getViewConfigs()).thenReturn(views);
      assertEquals(context(store).getMigrationCategory(), MigrationCategory.UNKNOWN);
    }
  }

  @Test(dataProvider = "malformedViews")
  public void testKnownCdcTakesPrecedenceOverMalformedViews(Map<String, ViewConfig> malformed) {
    for (boolean cdcFirst: new boolean[] { false, true }) {
      Map<String, ViewConfig> views = new LinkedHashMap<>();
      ViewConfig cdc = new ViewConfigImpl("com.linkedin.venice.views.ChangeCaptureView", Collections.emptyMap());
      if (cdcFirst) {
        views.put("cdc", cdc);
      }
      views.putAll(malformed);
      views.put("cdc", cdc);
      Store store = store(true, null, true, true, true);
      when(store.getViewConfigs()).thenReturn(views);
      assertEquals(context(store).getMigrationCategory(), MigrationCategory.CDC_VT);
    }
  }

  @Test
  public void testClassificationIsImmutableSnapshot() {
    Store store = store(false, "com.linkedin.venice.views.ChangeCaptureView", false, false, false);
    TopicPlacementContext context = context(store);
    store.getViewConfigs().clear();
    TopicPlacementContext batchContext = context(store);
    when(store.isHybrid()).thenReturn(true);
    assertEquals(context.getMigrationCategory(), MigrationCategory.CDC_VT);
    assertEquals(batchContext.getMigrationCategory(), MigrationCategory.BATCH_VT);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.HYBRID_VT);
  }

  @Test
  public void testMissingViewsAreEquivalentToEmptyViews() {
    Store store = store(false, null, false, false, false);
    when(store.getViewConfigs()).thenReturn(null);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.BATCH_VT);
    when(store.getIsDavinciHeartbeatReported()).thenReturn(true);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.DAVINCI_VT);
  }
}
