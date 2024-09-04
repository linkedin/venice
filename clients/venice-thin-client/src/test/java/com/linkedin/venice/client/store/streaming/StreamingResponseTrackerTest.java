package com.linkedin.venice.client.store.streaming;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.client.stats.ClientStats;
import org.testng.annotations.Test;


public class StreamingResponseTrackerTest {
  @Test
  public void testStreamingResponseTracker() {
    int keyCount = 100;
    ClientStats mockClientStats = mock(ClientStats.class);
    long startTimeNs = System.nanoTime();
    StreamingResponseTracker streamingResponseTracker =
        new StreamingResponseTracker(mockClientStats, keyCount, startTimeNs);
    streamingResponseTracker.recordReceived();
    verifyRecordStreamingResponseTime(mockClientStats, true, false, false, false, false);
    for (int i = 0; i < 48; i++) {
      streamingResponseTracker.recordReceived();
    }
    verifyRecordStreamingResponseTime(mockClientStats, true, false, false, false, false);
    streamingResponseTracker.recordReceived();
    verifyRecordStreamingResponseTime(mockClientStats, true, true, false, false, false);
    for (int i = 0; i < 48; i++) {
      streamingResponseTracker.recordReceived();
    }
    verifyRecordStreamingResponseTime(mockClientStats, true, true, true, true, false);
    streamingResponseTracker.recordReceived();
    verifyRecordStreamingResponseTime(mockClientStats, true, true, true, true, true);
  }

  @Test
  public void testStreamingResponseTrackerWithSmallKeyCount() {
    int keyCount = 2;
    ClientStats mockClientStats = mock(ClientStats.class);
    long startTimeNs = System.nanoTime();
    StreamingResponseTracker streamingResponseTracker =
        new StreamingResponseTracker(mockClientStats, keyCount, startTimeNs);
    streamingResponseTracker.recordReceived();
    // The key count ranges are calculated using integers (floor) so 2*99/100 = 1
    verifyRecordStreamingResponseTime(mockClientStats, true, true, true, true, true);
    streamingResponseTracker.recordReceived();
    verifyRecordStreamingResponseTime(mockClientStats, true, true, true, true, true);
  }

  private void verifyRecordStreamingResponseTime(
      ClientStats mockClientStats,
      boolean ttfr,
      boolean tt50pr,
      boolean tt90pr,
      boolean tt95pr,
      boolean tt99pr) {
    if (ttfr) {
      verify(mockClientStats, times(1)).recordStreamingResponseTimeToReceiveFirstRecord(anyDouble());
    } else {
      verify(mockClientStats, never()).recordStreamingResponseTimeToReceiveFirstRecord(anyDouble());
    }
    if (tt50pr) {
      verify(mockClientStats, times(1)).recordStreamingResponseTimeToReceive50PctRecord(anyDouble());
    } else {
      verify(mockClientStats, never()).recordStreamingResponseTimeToReceive50PctRecord(anyDouble());
    }
    if (tt90pr) {
      verify(mockClientStats, times(1)).recordStreamingResponseTimeToReceive90PctRecord(anyDouble());
    } else {
      verify(mockClientStats, never()).recordStreamingResponseTimeToReceive90PctRecord(anyDouble());
    }
    if (tt95pr) {
      verify(mockClientStats, times(1)).recordStreamingResponseTimeToReceive95PctRecord(anyDouble());
    } else {
      verify(mockClientStats, never()).recordStreamingResponseTimeToReceive95PctRecord(anyDouble());
    }
    if (tt99pr) {
      verify(mockClientStats, times(1)).recordStreamingResponseTimeToReceive99PctRecord(anyDouble());
    } else {
      verify(mockClientStats, never()).recordStreamingResponseTimeToReceive99PctRecord(anyDouble());
    }
  }
}
