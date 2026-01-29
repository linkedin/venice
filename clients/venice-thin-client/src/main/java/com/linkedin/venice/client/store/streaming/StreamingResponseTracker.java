package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This class provides percentile tracking and stats emission for streaming response
 */
public class StreamingResponseTracker {
  private final ClientStats stats;
  private final int keyCntForP50;
  private final int keyCntForP90;
  private final long startTimeInNS;
  private final AtomicInteger receivedKeyCnt = new AtomicInteger(0);

  public StreamingResponseTracker(ClientStats stats, int keyCnt, long startTimeInNS) {
    this.stats = stats;
    this.keyCntForP50 = keyCnt / 2;
    this.keyCntForP90 = keyCnt * 9 / 10;
    this.startTimeInNS = startTimeInNS;
  }

  public void recordReceived() {
    int currentKeyCnt = receivedKeyCnt.incrementAndGet();
    /**
     * Do not short-circuit because the key cnt for each percentile could be the same if the total key count is small
     */
    if (currentKeyCnt == 1) {
      stats.recordStreamingResponseTimeToReceiveFirstRecord(LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS));
    }
    if (currentKeyCnt == keyCntForP50) {
      stats.recordStreamingResponseTimeToReceive50PctRecord(LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS));
    }
    if (currentKeyCnt == keyCntForP90) {
      stats.recordStreamingResponseTimeToReceive90PctRecord(LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS));
    }
  }
}
