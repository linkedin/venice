package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.Store;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Targeted unit tests for the static decision helpers on {@link StoreIngestionTask} that drive
 * the run-time pause transition in {@code LeaderFollowerStoreIngestionTask.maybeTransitionPauseState}
 * and the post-subscribe restart-while-paused hook in {@code validateAndSubscribePartition}.
 * Helpers under test:
 * <ul>
 *   <li>{@link StoreIngestionTask#shouldPauseForStore(Store, int)}</li>
 *   <li>{@link StoreIngestionTask#shouldSkipQuotaCallbackForStoreLevelPause}</li>
 *   <li>{@link StoreIngestionTask#shouldUnsubscribeOnStartup}</li>
 * </ul>
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

  private PartitionConsumptionState pcsAlreadyPaused(boolean alreadyPaused) {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.isStoreLevelPaused()).thenReturn(alreadyPaused);
    return pcs;
  }

  // ----- shouldPauseForStore -----

  /**
   * Truth table for shouldPauseForStore.
   * Columns: pauseMode, currentVersion, sitVersion, expected, label.
   */
  @DataProvider(name = "shouldPauseCases")
  public Object[][] shouldPauseCases() {
    return new Object[][] {
        // NOT_PAUSED / null -> never pause
        { IngestionPauseMode.NOT_PAUSED, CURRENT_VERSION, CURRENT_VERSION, false, "NOT_PAUSED" },
        { null, CURRENT_VERSION, CURRENT_VERSION, false, "null mode (older schema)" },
        // ALL_VERSIONS -> always pause
        { IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION, CURRENT_VERSION, true, "ALL_VERSIONS, current" },
        { IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION, NON_CURRENT_VERSION, true, "ALL_VERSIONS, non-current" },
        // CURRENT_VERSION -> only when SIT version matches
        { IngestionPauseMode.CURRENT_VERSION, CURRENT_VERSION, CURRENT_VERSION, true, "CURRENT_VERSION, current" },
        { IngestionPauseMode.CURRENT_VERSION, CURRENT_VERSION, NON_CURRENT_VERSION, false,
            "CURRENT_VERSION, non-current" } };
  }

  @Test(dataProvider = "shouldPauseCases")
  public void shouldPauseForStoreTruthTable(
      IngestionPauseMode mode,
      int currentVersion,
      int sitVersion,
      boolean expected,
      String label) {
    Store store = storeWithPauseMode(mode, currentVersion);
    assertEquals(StoreIngestionTask.shouldPauseForStore(store, sitVersion), expected, label);
  }

  @Test
  public void currentVersionSwapPausesNewCurrentResumesOldCurrent() {
    // Simulates a current-version swap (V4 -> V5) under CURRENT_VERSION mode. After the swap,
    // the now-backup V4's SIT should no longer be paused and the newly-current V5's SIT should
    // start pausing. Codifies dynamic-rebalance behavior — the static truth table above only
    // covers a single snapshot, so this test mutates the mocked currentVersion to verify the
    // decision flips on swap.
    Store store = mock(Store.class);
    when(store.getIngestionPauseMode()).thenReturn(IngestionPauseMode.CURRENT_VERSION);

    when(store.getCurrentVersion()).thenReturn(4);
    assertEquals(StoreIngestionTask.shouldPauseForStore(store, 4), true, "V4 (current) -> paused");
    assertEquals(StoreIngestionTask.shouldPauseForStore(store, 5), false, "V5 (future) -> not paused");

    when(store.getCurrentVersion()).thenReturn(5);
    assertEquals(StoreIngestionTask.shouldPauseForStore(store, 4), false, "V4 (backup after swap) -> not paused");
    assertEquals(StoreIngestionTask.shouldPauseForStore(store, 5), true, "V5 (current after swap) -> paused");
  }

  // ----- shouldSkipQuotaCallbackForStoreLevelPause -----

  /**
   * Truth table for the disk-quota callback no-op guard.
   * Columns: pcsState (Boolean, null = pass null PCS), expected, label.
   */
  @DataProvider(name = "quotaCallbackCases")
  public Object[][] quotaCallbackCases() {
    return new Object[][] { { Boolean.TRUE, true, "store-level paused -> skip" },
        { Boolean.FALSE, false, "not store-level paused -> proceed" },
        // PCS can be null if UNSUBSCRIBE tore it down before the quota callback fires.
        { null, false, "null PCS -> proceed" } };
  }

  @Test(dataProvider = "quotaCallbackCases")
  public void shouldSkipQuotaCallbackTruthTable(Boolean pcsPaused, boolean expected, String label) {
    PartitionConsumptionState pcs = pcsPaused == null ? null : pcsAlreadyPaused(pcsPaused);
    assertEquals(StoreIngestionTask.shouldSkipQuotaCallbackForStoreLevelPause(pcs), expected, label);
  }

  // ----- shouldUnsubscribeOnStartup (restart-while-paused decision) -----

  /**
   * Truth table for the restart-while-paused decision.
   * Columns: pcsState (Boolean, null = pass null PCS), pauseMode (null = pass null Store),
   *          sitVersion, expected, label.
   */
  @DataProvider(name = "unsubscribeOnStartupCases")
  public Object[][] unsubscribeOnStartupCases() {
    return new Object[][] {
        // Happy path: store paused, PCS fresh
        { Boolean.FALSE, IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION, true, "fresh PCS + ALL_VERSIONS" },
        // Already paused -> don't double-unsubscribe
        { Boolean.TRUE, IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION, false, "already paused PCS" },
        // Store not paused -> nothing to do
        { Boolean.FALSE, IngestionPauseMode.NOT_PAUSED, CURRENT_VERSION, false, "store NOT_PAUSED" },
        // Defensive null guards
        { null, IngestionPauseMode.ALL_VERSIONS, CURRENT_VERSION, false, "null PCS" },
        { Boolean.FALSE, null, CURRENT_VERSION, false, "null Store (transient lookup miss)" },
        // CURRENT_VERSION mode interacts with sitVersion
        { Boolean.FALSE, IngestionPauseMode.CURRENT_VERSION, CURRENT_VERSION, true, "CURRENT_VERSION mode, current" },
        { Boolean.FALSE, IngestionPauseMode.CURRENT_VERSION, NON_CURRENT_VERSION, false,
            "CURRENT_VERSION mode, future version" } };
  }

  @Test(dataProvider = "unsubscribeOnStartupCases")
  public void shouldUnsubscribeOnStartupTruthTable(
      Boolean pcsState,
      IngestionPauseMode mode,
      int sitVersion,
      boolean expected,
      String label) {
    PartitionConsumptionState pcs = pcsState == null ? null : pcsAlreadyPaused(pcsState);
    Store store = mode == null ? null : storeWithPauseMode(mode, CURRENT_VERSION);
    assertEquals(StoreIngestionTask.shouldUnsubscribeOnStartup(pcs, store, sitVersion), expected, label);
  }
}
