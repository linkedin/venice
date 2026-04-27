package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.ENTER_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.EXIT_PAUSE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.PauseStateTransition.NO_CHANGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


/**
 * Unit tests for {@link LeaderFollowerStoreIngestionTask#decidePauseTransition} — the pure
 * decision function that drives {@code maybeTransitionPauseState}'s per-PCS state machine.
 * Covers all four (shouldPause × isStoreLevelPaused) combinations.
 */
public class LeaderFollowerStoreIngestionTaskPauseTransitionTest {
  private PartitionConsumptionState pcsWithPauseFlag(boolean storeLevelPaused) {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.isStoreLevelPaused()).thenReturn(storeLevelPaused);
    return pcs;
  }

  @Test
  public void enterPauseWhenShouldPauseAndNotYetPaused() {
    assertEquals(LeaderFollowerStoreIngestionTask.decidePauseTransition(pcsWithPauseFlag(false), true), ENTER_PAUSE);
  }

  @Test
  public void exitPauseWhenShouldNotPauseAndCurrentlyPaused() {
    assertEquals(LeaderFollowerStoreIngestionTask.decidePauseTransition(pcsWithPauseFlag(true), false), EXIT_PAUSE);
  }

  @Test
  public void noChangeWhenAlreadyPausedAndShouldStayPaused() {
    assertEquals(LeaderFollowerStoreIngestionTask.decidePauseTransition(pcsWithPauseFlag(true), true), NO_CHANGE);
  }

  @Test
  public void noChangeWhenNotPausedAndShouldStayUnpaused() {
    assertEquals(LeaderFollowerStoreIngestionTask.decidePauseTransition(pcsWithPauseFlag(false), false), NO_CHANGE);
  }
}
