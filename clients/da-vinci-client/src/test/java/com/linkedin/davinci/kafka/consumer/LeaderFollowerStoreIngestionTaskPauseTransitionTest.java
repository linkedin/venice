package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.ENTER_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.EXIT_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.NO_CHANGE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.RECONCILE_FORCE_UNSUBSCRIBE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link LeaderFollowerStoreIngestionTask#decidePauseTransition} — the pure
 * decision function that drives {@code maybeTransitionPauseState}'s per-PCS state machine.
 * Covers the full (PCS state × shouldPause × hasAnyActiveSubscription) truth table including
 * the null-PCS guard and the {@code RECONCILE_FORCE_UNSUBSCRIBE} branch (paused PCS with a
 * subscription that crept back).
 */
public class LeaderFollowerStoreIngestionTaskPauseTransitionTest {
  /**
   * Truth table for decidePauseTransition.
   * Columns: pcsAlreadyPaused (Boolean, null = pass null PCS), shouldPause,
   *          hasAnyActiveSubscription, expectedTransition, label.
   */
  @DataProvider(name = "transitionCases")
  public Object[][] transitionCases() {
    return new Object[][] {
        // Active transitions
        { Boolean.FALSE, true, false, ENTER_PAUSE, "fresh PCS + shouldPause -> ENTER" },
        { Boolean.FALSE, true, true, ENTER_PAUSE,
            "fresh PCS + shouldPause + sub already exists (won't yet, but covers ordering) -> ENTER" },
        { Boolean.TRUE, false, false, EXIT_PAUSE, "paused PCS + !shouldPause -> EXIT" },
        { Boolean.TRUE, false, true, EXIT_PAUSE,
            "paused PCS + !shouldPause + crept-back sub -> EXIT (resume reattaches anyway)" },
        // Reconcile path: paused flag + still subscribed under shouldPause
        { Boolean.TRUE, true, true, RECONCILE_FORCE_UNSUBSCRIBE,
            "paused PCS + shouldPause + sub crept back -> RECONCILE_FORCE_UNSUBSCRIBE" },
        // Steady-state no-ops
        { Boolean.TRUE, true, false, NO_CHANGE, "already paused, no sub crept back -> NO_CHANGE" },
        { Boolean.FALSE, false, false, NO_CHANGE, "not paused + !shouldPause -> NO_CHANGE" },
        { Boolean.FALSE, false, true, NO_CHANGE,
            "not paused + !shouldPause + dangling sub (won't happen normally) -> NO_CHANGE" },
        // Defensive null-PCS guard
        { null, true, true, NO_CHANGE, "null PCS + shouldPause + sub -> NO_CHANGE" },
        { null, true, false, NO_CHANGE, "null PCS + shouldPause + no sub -> NO_CHANGE" },
        { null, false, false, NO_CHANGE, "null PCS + !shouldPause + no sub -> NO_CHANGE" } };
  }

  @Test(dataProvider = "transitionCases")
  public void decidePauseTransitionTruthTable(
      Boolean pcsAlreadyPaused,
      boolean shouldPause,
      boolean hasAnyActiveSubscription,
      PauseStateTransition expected,
      String label) {
    PartitionConsumptionState pcs = null;
    if (pcsAlreadyPaused != null) {
      pcs = mock(PartitionConsumptionState.class);
      when(pcs.isStoreLevelPaused()).thenReturn(pcsAlreadyPaused);
    }
    assertEquals(
        LeaderFollowerStoreIngestionTask.decidePauseTransition(pcs, shouldPause, hasAnyActiveSubscription),
        expected,
        label);
  }
}
