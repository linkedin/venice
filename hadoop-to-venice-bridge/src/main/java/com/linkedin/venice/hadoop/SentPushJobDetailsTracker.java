package com.linkedin.venice.hadoop;

import com.linkedin.venice.status.protocol.PushJobDetails;


/**
 * Interface of class that is used to keep track of push job details sent to the Venice controller.
 */
public interface SentPushJobDetailsTracker {
  void record(String storeName, int version, PushJobDetails pushJobDetails);
}
