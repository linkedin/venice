package com.linkedin.venice.pubsub;

import java.time.Duration;


public class PubSubConstantsOverrider {
  public static void setPubsubOffsetApiTimeoutDurationDefaultValue(Duration duration) {
    PubSubConstants.setPubsubOffsetApiTimeoutDurationDefaultValue(duration);
  }

  public static void resetPubsubOffsetApiTimeoutDurationDefaultValue() {
    PubSubConstants.resetPubsubOffsetApiTimeoutDurationDefaultValue();
  }
}
