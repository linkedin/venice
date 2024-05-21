package com.linkedin.venice.pubsub;

import java.time.Duration;


/**
 * The purpose of this class is only to make the package private test-only functions of {@link PubSubConstants}
 * available to all packages within tests. DO NOT move this class outside the test module, as we do not want to make
 * the test-only functions more widely available.
 */
public class PubSubConstantsOverrider {
  public static void setPubsubOffsetApiTimeoutDurationDefaultValue(Duration duration) {
    PubSubConstants.setPubsubOffsetApiTimeoutDurationDefaultValue(duration);
  }

  public static void resetPubsubOffsetApiTimeoutDurationDefaultValue() {
    PubSubConstants.resetPubsubOffsetApiTimeoutDurationDefaultValue();
  }
}
