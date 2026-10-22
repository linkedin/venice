package com.linkedin.venice.controller.placement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.placement.TopicPlacementContext.MigrationCategory;
import com.linkedin.venice.meta.Store;
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
  public void testCategoryUsesOnlyIngestionMode(boolean hybrid, MigrationCategory expected) {
    Store store = store(hybrid);
    assertEquals(context(store).getMigrationCategory(), expected);
    verify(store).isHybrid();
    verifyNoMoreInteractions(store);
  }

  @Test
  public void testCompatibilityConstructorIsUnverified() {
    TopicPlacementContext context = new TopicPlacementContext("cluster", "store", "store_v1", null, true);
    assertEquals(context.getMigrationCategory(), MigrationCategory.UNKNOWN);
    assertEquals(context(null).getMigrationCategory(), MigrationCategory.UNKNOWN);
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
