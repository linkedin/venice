package com.linkedin.venice.hadoop;

import com.linkedin.venice.status.protocol.PushJobDetails;


public class NoOpSentPushJobDetailsTracker implements SentPushJobDetailsTracker {
  @Override
  public void record(String storeName, int version, PushJobDetails pushJobDetails) {
    // No op
  }
}
