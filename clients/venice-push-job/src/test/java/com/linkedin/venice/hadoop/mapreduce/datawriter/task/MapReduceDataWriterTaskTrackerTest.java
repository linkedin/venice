package com.linkedin.venice.hadoop.mapreduce.datawriter.task;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
    Reporter reporter = mock(Reporter.class);
    doAnswer(invocation -> {
      counters.incrCounter(
          (String) invocation.getArgument(0),
          (String) invocation.getArgument(1),
          (Long) invocation.getArgument(2));
      return null;
    }).when(reporter).incrCounter(anyString(), anyString(), anyLong());

    ReporterBackedMapReduceDataWriterTaskTracker reporterTracker =
        new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    reporterTracker.trackFailedExternalStorageRegion("dc-0");
    reporterTracker.trackFailedExternalStorageRegion("dc-1");
    reporterTracker.trackFailedExternalStorageRegion("dc-0");

    Set<String> expected = new HashSet<>();
    expected.add("dc-0");
    expected.add("dc-1");
    assertEquals(reporterTracker.getFailedExternalStorageRegions(), expected);

    CounterBackedMapReduceDataWriterTaskTracker counterTracker =
        new CounterBackedMapReduceDataWriterTaskTracker(counters);
    assertEquals(counterTracker.getFailedExternalStorageRegions(), expected);
    assertThrows(
        UnsupportedOperationException.class,
        () -> counterTracker.getFailedExternalStorageRegions().add("dc-2"));
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
}
