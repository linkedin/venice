package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import org.testng.annotations.Test;


/**
 * Targeted unit tests for {@link StoreIngestionTask#shouldPauseForStore(Store, int)} —
 * the static decision helper used by both the run-time pause transition (in
 * {@code LeaderFollowerStoreIngestionTask.maybeTransitionPauseState}) and the post-subscribe
 * restart-while-paused hook in {@code StoreIngestionTask.validateAndSubscribePartition}.
 */
public class StoreIngestionTaskShouldPauseTest {
  private static final int CURRENT_VERSION = 5;
  private static final int NON_CURRENT_VERSION = 4;

  private Store storeWithPauseMode(IngestionPauseMode mode, int currentVersion) {
    Store store = mock(Store.class);
    when(store.getIngestionPauseMode()).thenReturn(mode);
    when(store.getCurrentVersion()).thenReturn(currentVersion);
    return store;
  }

  @Test
  public void notPausedReturnsFalse() {
    Store store = storeWithPauseMode(IngestionPauseMode.NOT_PAUSED, CURRENT_VERSION);
    assertFalse(StoreIngestionTask.shouldPauseForStore(store, CURRENT_VERSION));
  }

  @Test
  public void nullModeReturnsFalse() {
    // Defensive: a store metadata fetch on an older schema could return null.
    Store store = storeWithPauseMode(null, CURRENT_VERSION);
    assertFalse(StoreIngestionTask.shouldPauseForStore(store, CURRENT_VERSION));
  }

  @Test
  public void allVersionsAlwaysPauses() {
    Store store = storeWithPauseMode(IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION);
    assertTrue(StoreIngestionTask.shouldPauseForStore(store, CURRENT_VERSION));
    assertTrue(StoreIngestionTask.shouldPauseForStore(store, NON_CURRENT_VERSION));
  }

  @Test
  public void currentVersionPausesOnlyCurrent() {
    Store store = storeWithPauseMode(IngestionPauseMode.CURRENT_VERSION, CURRENT_VERSION);
    assertTrue(StoreIngestionTask.shouldPauseForStore(store, CURRENT_VERSION));
    assertFalse(StoreIngestionTask.shouldPauseForStore(store, NON_CURRENT_VERSION));
  }

  @Test
  public void quotaCallbackSkipsWhenStoreLevelPaused() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.isStoreLevelPaused()).thenReturn(true);
    assertTrue(StoreIngestionTask.shouldSkipQuotaCallbackForStoreLevelPause(pcs));
  }

  @Test
  public void quotaCallbackProceedsWhenNotStoreLevelPaused() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.isStoreLevelPaused()).thenReturn(false);
    assertFalse(StoreIngestionTask.shouldSkipQuotaCallbackForStoreLevelPause(pcs));
  }

  @Test
  public void quotaCallbackProceedsWhenPcsIsNull() {
    // Defensive: partitionConsumptionStateMap.get(partitionId) can return null if a UNSUBSCRIBE
    // has already torn down the PCS by the time the quota callback fires.
    assertFalse(StoreIngestionTask.shouldSkipQuotaCallbackForStoreLevelPause(null));
  }
}
