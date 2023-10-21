package com.linkedin.venice.utils;

import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubConstants;


public class StoreUtils {
  /**
   * The default retention time for the RT topic is defined in {@link PubSubConstants#DEFAULT_TOPIC_RETENTION_POLICY_MS},
   * but if the rewind time is larger than this, then the RT topic's retention time needs to be set even higher,
   * in order to guarantee that buffer replays do not lose data. In order to achieve this, the retention time is
   * set to the max of either:
   *
   * 1. {@link PubSubConstants#DEFAULT_TOPIC_RETENTION_POLICY_MS}; or
   * 2. {@link HybridStoreConfig#getRewindTimeInSeconds()} + {@link Store#getBootstrapToOnlineTimeoutInHours()} + {@value PubSubConstants#BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN};
   *
   * This is a convenience function, and thus must be ignored by the JSON machinery.
   *
   * @return the retention time for the RT topic, in milliseconds.
   */
  public static long getExpectedRetentionTimeInMs(Store store, HybridStoreConfig hybridConfig) {
    long rewindTimeInMs = hybridConfig.getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    long bootstrapToOnlineTimeInMs = (long) store.getBootstrapToOnlineTimeoutInHours() * Time.MS_PER_HOUR;
    long minimumRetentionInMs =
        rewindTimeInMs + bootstrapToOnlineTimeInMs + PubSubConstants.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN;
    return Math.max(minimumRetentionInMs, PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }
}
