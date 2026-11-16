package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.IngestionShutdownDiagnostics.Phase;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;


public class IngestionShutdownDiagnosticsTest {
  @Test
  public void testMonotonicPhaseTotalsExcludeCurrentOperation() {
    AtomicLong nanos = new AtomicLong();
    IngestionShutdownDiagnostics diagnostics = new IngestionShutdownDiagnostics(nanos::get);
    diagnostics.setPartitions(2);
    nanos.set(TimeUnit.SECONDS.toNanos(57));
    diagnostics.transition(Phase.WRITER_CLOSE, 1);
    diagnostics.transition(Phase.WRITER_LOCK, 1);
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(20));
    diagnostics.transition(Phase.EOS_SUBMIT, 1);
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(30));
    diagnostics.transition(Phase.EOS_FUTURE, 1);
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(40));

    String snapshot = diagnostics.snapshot();
    assertTrue(snapshot.contains("phase=EOS_FUTURE, partition=1, phaseElapsedMs=40"));
    assertTrue(snapshot.contains("CLOSE_REQUEST_TO_LOOP_EXIT[finishedVisits=1, totalMs=57000, maxMs=57000]"));
    assertTrue(snapshot.contains("WRITER_LOCK[finishedVisits=1, totalMs=20, maxMs=20]"));
    assertTrue(snapshot.contains("EOS_SUBMIT[finishedVisits=1, totalMs=30, maxMs=30]"));
    assertTrue(snapshot.contains("EOS_FUTURE[finishedVisits=0, totalMs=0, maxMs=0]"));

    diagnostics.transition(Phase.WRITER_CLOSE, 1);
    diagnostics.transition(Phase.WRITER_LOCK, 0);
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    diagnostics.transition(Phase.EOS_SUBMIT, 0);
    assertTrue(diagnostics.snapshot().contains("WRITER_LOCK[finishedVisits=2, totalMs=30, maxMs=20]"));
    diagnostics.transition(Phase.FINISHED, -1);
    String finished = diagnostics.snapshot();
    nanos.addAndGet(TimeUnit.SECONDS.toNanos(10));
    assertTrue(diagnostics.isFinished());
    assertEquals(diagnostics.snapshot(), finished);
  }

  @Test
  public void testInclusiveWriterTotalsAndBoundedSlowDetails() {
    IngestionShutdownDiagnostics diagnostics = new IngestionShutdownDiagnostics();
    diagnostics.setPartitions(8);
    assertFalse(diagnostics.writerCloseFinished(TimeUnit.MILLISECONDS.toNanos(999), false));
    for (int i = 0; i < 7; i++) {
      boolean shouldLog = diagnostics.writerCloseFinished(TimeUnit.SECONDS.toNanos(2), i == 0);
      if (i < 5) {
        assertTrue(shouldLog);
      } else {
        assertFalse(shouldLog);
      }
      diagnostics.writerSweepPartitionFinished();
    }
    diagnostics.checkpointFinished();
    String snapshot = diagnostics.snapshot();
    assertTrue(snapshot.contains("writerClosesFinished=8, writerClosesFailed=1"));
    assertTrue(snapshot.contains("writerSweepPartitionsFinished=7, writerSweepPartitionsRemaining=1"));
    assertTrue(snapshot.contains("writerCloseInclusiveTotalMs=14999, writerCloseInclusiveMaxMs=2000"));
    assertTrue(snapshot.contains("checkpointsFinished=1, checkpointsRemaining=7"));
  }
}
