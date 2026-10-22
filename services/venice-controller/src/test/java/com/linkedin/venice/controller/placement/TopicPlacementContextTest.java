package com.linkedin.venice.controller.placement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.placement.TopicPlacementContext.MigrationCategory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TopicPlacementContextTest {
  private TopicPlacementContext context(Store store) {
    return new TopicPlacementContext("cluster", "store", "store_v1", "destination", true, store);
  }

  private Store store(boolean hybrid) {
    Store store = mock(Store.class);
    when(store.isHybrid()).thenReturn(hybrid);
    return store;
  }

  @DataProvider
  public Object[][] categories() {
    return new Object[][] { { false, MigrationCategory.BATCH_VT }, { true, MigrationCategory.HYBRID_VT } };
  }

  @Test(dataProvider = "categories")
  public void testMetadataClassification(boolean hybrid, MigrationCategory expected) {
    assertEquals(context(store(hybrid)).getMigrationCategory(), expected);
  }

  @Test
  public void testMigrationCategoriesDescribeOnlyIngestionMode() {
    assertEquals(
        EnumSet.allOf(MigrationCategory.class),
        EnumSet.of(MigrationCategory.UNKNOWN, MigrationCategory.BATCH_VT, MigrationCategory.HYBRID_VT));
  }

  @DataProvider
  public Object[][] consumerFlags() {
    return new Object[][] { { false, false, false }, { true, false, false }, { false, true, false },
        { false, false, true }, { true, true, true } };
  }

  @Test(dataProvider = "consumerFlags")
  public void testConsumerFlagsDoNotDetermineCategory(boolean pushStatus, boolean heartbeat, boolean blob) {
    for (boolean hybrid: new boolean[] { false, true }) {
      Store store = store(hybrid);
      when(store.isDaVinciPushStatusStoreEnabled()).thenReturn(pushStatus);
      when(store.getIsDavinciHeartbeatReported()).thenReturn(heartbeat);
      when(store.isBlobTransferEnabled()).thenReturn(blob);
      assertEquals(
          context(store).getMigrationCategory(),
          hybrid ? MigrationCategory.HYBRID_VT : MigrationCategory.BATCH_VT);
      verify(store, never()).isDaVinciPushStatusStoreEnabled();
      verify(store, never()).getIsDavinciHeartbeatReported();
      verify(store, never()).isBlobTransferEnabled();
    }
  }

  @Test
  public void testCompatibilityConstructorIsUnverified() {
    TopicPlacementContext context = new TopicPlacementContext("cluster", "store", "store_v1", null, true);
    assertEquals(context.getMigrationCategory(), MigrationCategory.UNKNOWN);
    assertEquals(context(null).getMigrationCategory(), MigrationCategory.UNKNOWN);
  }

  @DataProvider
  public Object[][] views() {
    return new Object[][] { { null }, { Collections.emptyMap() },
        { Collections.singletonMap(
            "view",
            new ViewConfigImpl("com.linkedin.venice.views.ChangeCaptureView", Collections.emptyMap())) },
        { Collections.singletonMap("view", null) },
        { Collections.singletonMap("view", new ViewConfigImpl(null, Collections.emptyMap())) },
        { Collections.singletonMap("view", new ViewConfigImpl(" ", Collections.emptyMap())) },
        { Collections.singletonMap("view", new ViewConfigImpl(null)) } };
  }

  @Test(dataProvider = "views")
  public void testViewMetadataDoesNotDetermineCategory(Map<String, ViewConfig> views) {
    Store store = store(false);
    when(store.getViewConfigs()).thenReturn(views);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.BATCH_VT);
    when(store.isHybrid()).thenReturn(true);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.HYBRID_VT);
    verify(store, never()).getViewConfigs();
  }

  @Test
  public void testClassificationIsImmutableSnapshot() {
    Store store = store(true);
    TopicPlacementContext context = context(store);
    when(store.isHybrid()).thenReturn(false);
    TopicPlacementContext batchContext = context(store);
    when(store.isHybrid()).thenReturn(true);
    assertEquals(context.getMigrationCategory(), MigrationCategory.HYBRID_VT);
    assertEquals(batchContext.getMigrationCategory(), MigrationCategory.BATCH_VT);
    assertEquals(context(store).getMigrationCategory(), MigrationCategory.HYBRID_VT);
  }
}
