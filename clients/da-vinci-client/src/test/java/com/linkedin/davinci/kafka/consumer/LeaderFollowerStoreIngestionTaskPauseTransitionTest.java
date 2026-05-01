package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.ENTER_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.EXIT_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.NO_CHANGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link LeaderFollowerStoreIngestionTask#decidePauseTransition} — the pure
 * decision function that drives {@code maybeTransitionPauseState}'s per-PCS state machine.
 * Covers the full (PCS state × shouldPause) truth table, including the null-PCS guard.
 */
public class LeaderFollowerStoreIngestionTaskPauseTransitionTest {
  /**
   * Truth table for decidePauseTransition.
   * Columns: pcsAlreadyPaused (Boolean, null = pass null PCS), shouldPause, expectedTransition, label.
   */
  @DataProvider(name = "transitionCases")
  public Object[][] transitionCases() {
    return new Object[][] {
        // Active transitions
        { Boolean.FALSE, true, ENTER_PAUSE, "fresh PCS + shouldPause -> ENTER" },
        { Boolean.TRUE, false, EXIT_PAUSE, "paused PCS + !shouldPause -> EXIT" },
        // Steady-state no-ops
        { Boolean.TRUE, true, NO_CHANGE, "already paused + still shouldPause -> NO_CHANGE" },
        { Boolean.FALSE, false, NO_CHANGE, "not paused + !shouldPause -> NO_CHANGE" },
        // Defensive null-PCS guard (mirrors shouldSkipQuotaCallbackForStoreLevelPause)
        { null, true, NO_CHANGE, "null PCS + shouldPause -> NO_CHANGE" },
        { null, false, NO_CHANGE, "null PCS + !shouldPause -> NO_CHANGE" } };
  }

  @Test(dataProvider = "transitionCases")
  public void decidePauseTransitionTruthTable(
      Boolean pcsAlreadyPaused,
      boolean shouldPause,
      PauseStateTransition expected,
      String label) {
    PartitionConsumptionState pcs = null;
    if (pcsAlreadyPaused != null) {
      pcs = mock(PartitionConsumptionState.class);
      when(pcs.isStoreLevelPaused()).thenReturn(pcsAlreadyPaused);
    }
    assertEquals(LeaderFollowerStoreIngestionTask.decidePauseTransition(pcs, shouldPause), expected, label);
  }
}
