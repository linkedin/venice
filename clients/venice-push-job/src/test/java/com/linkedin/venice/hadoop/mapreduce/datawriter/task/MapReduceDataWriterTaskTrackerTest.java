package com.linkedin.venice.hadoop.mapreduce.datawriter.task;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.testng.annotations.Test;


public class MapReduceDataWriterTaskTrackerTest {
  @Test
  public void testFailedExternalStorageRegionsAggregateFromReporterIntoCounters() {
    Counters counters = new Counters();
    Reporter reporter = countingReporter(counters);

    ReporterBackedMapReduceDataWriterTaskTracker reporterTracker =
        new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    reporterTracker.trackFailedExternalStorageRegion("dc-0");
    reporterTracker.trackFailedExternalStorageRegion("dc-1");
    reporterTracker.trackFailedExternalStorageRegion("dc-0");

    Set<String> expected = new HashSet<>();
    expected.add("dc-0");
    expected.add("dc-1");
    assertEquals(reporterTracker.getFailedExternalStorageRegions(), expected);
    verify(reporter, times(2)).incrCounter(anyString(), anyString(), anyLong());

    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(counters);
    assertEquals(counterTracker.getFailedExternalStorageRegions(), expected);
    assertThrows(
        UnsupportedOperationException.class,
        () -> counterTracker.getFailedExternalStorageRegions().add("dc-2"));
  }

  @Test
  public void testWriteTimesRoundTripFromReporterIntoCounters() {
    Counters counters = new Counters();
    Reporter reporter = countingReporter(counters);

    ReporterBackedMapReduceDataWriterTaskTracker reporterTracker =
        new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    when(reporter.getCounter(anyString(), anyString())).thenAnswer(
        invocation -> counters.findCounter((String) invocation.getArgument(0), (String) invocation.getArgument(1)));

    // Several tasks/batches report incrementally; the counter is the sum of every report.
    reporterTracker.trackExternalStorageWriteTime(120);
    reporterTracker.trackExternalStorageWriteTime(30);
    reporterTracker.trackVeniceWriteTime(7);

    assertEquals(reporterTracker.getExternalStorageWriteTimeMs(), 150L);
    assertEquals(reporterTracker.getVeniceWriteTimeMs(), 7L);

    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(counters);
    assertEquals(counterTracker.getExternalStorageWriteTimeMs(), 150L);
    assertEquals(counterTracker.getVeniceWriteTimeMs(), 7L);
  }

  @Test
  public void testWriteTimesDefaultToZeroWithoutAnyReport() {
    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(new Counters());
    assertEquals(counterTracker.getExternalStorageWriteTimeMs(), 0L);
    assertEquals(counterTracker.getVeniceWriteTimeMs(), 0L);

    ReporterBackedMapReduceDataWriterTaskTracker nullReporterTracker =
        new ReporterBackedMapReduceDataWriterTaskTracker(null);
    // Reporter.NULL has no counters to read or write; reporting must be a no-op rather than a failure.
    nullReporterTracker.trackExternalStorageWriteTime(10);
    nullReporterTracker.trackVeniceWriteTime(10);
    assertEquals(nullReporterTracker.getExternalStorageWriteTimeMs(), 0L);
    assertEquals(nullReporterTracker.getVeniceWriteTimeMs(), 0L);
  }

  @Test
  public void testFailedExternalStorageRegionsDefaultToEmpty() {
    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(new Counters());
    assertEquals(counterTracker.getFailedExternalStorageRegions(), Collections.emptySet());
  }

  @Test
  public void testFailedExternalStorageRegionsDefaultToEmptyWhenCounterGroupIsMissing() {
    Counters counters = mock(Counters.class);
    when(counters.getGroup(anyString())).thenReturn(null);

    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(counters);
    assertEquals(counterTracker.getFailedExternalStorageRegions(), Collections.emptySet());
  }

  /** A {@link Reporter} mock whose counter increments land in {@code counters}, like a real MR task's do. */
  private static Reporter countingReporter(Counters counters) {
    Reporter reporter = mock(Reporter.class);
    doAnswer(invocation -> {
      counters.incrCounter(
          (String) invocation.getArgument(0),
          (String) invocation.getArgument(1),
          (Long) invocation.getArgument(2));
      return null;
    }).when(reporter).incrCounter(anyString(), anyString(), anyLong());
    return reporter;
  }
}
