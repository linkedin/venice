package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class PeriodicCheckpointTest {
  @Test
  public void testTimeRequiresPositiveExistingBytesAndStableSpread() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    assertFalse(checkpoint.isDue(1000, 0));
    assertFalse(checkpoint.isDue(119, 1));
    assertTrue(checkpoint.isDue(120, 1));
    assertFalse(checkpoint.isDue(120, 0), "Dispatch reset suppresses time even before durable completion");
    assertTrue(checkpoint.isDue(121, 1), "New bytes need no separate in-flight reservation");
  }

  @Test
  public void testEverySuccessfulCheckpointRefreshesDeadlineWithSameSpread() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 20, 0);
    checkpoint.checkpointSucceeded(50); // A byte/control checkpoint before the deadline.
    assertFalse(checkpoint.isDue(169, 1));
    assertTrue(checkpoint.isDue(170, 1));
    checkpoint.checkpointSucceeded(1000); // A late success does not replay missed intervals.
    assertFalse(checkpoint.isDue(1119, 1));
    assertTrue(checkpoint.isDue(1120, 1));
    assertFalse(checkpoint.isDue(2000, 0));
  }

  @Test
  public void testFailureOrSkipWithoutSuccessLeavesDeadlineUnchanged() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, 0);
    assertTrue(checkpoint.isDue(100, 1));
    assertFalse(checkpoint.isDue(101, 0), "Global dispatch resets bytes even if persistence fails");
    assertTrue(checkpoint.isDue(102, 1), "Failure does not postpone the deadline when bytes arrive again");
  }

  @Test
  public void testMonotonicClockWraparound() {
    PeriodicCheckpoint checkpoint = new PeriodicCheckpoint(100, 0, Long.MAX_VALUE - 50);
    assertFalse(checkpoint.isDue(Long.MIN_VALUE + 48, 1));
    assertTrue(checkpoint.isDue(Long.MIN_VALUE + 49, 1));
    checkpoint.checkpointSucceeded(Long.MIN_VALUE + 50);
    assertFalse(checkpoint.isDue(Long.MIN_VALUE + 149, 1));
    assertTrue(checkpoint.isDue(Long.MIN_VALUE + 150, 1));
  }
}
