package com.linkedin.davinci.consumer;

/**
 * Receives a {@link VersionSwapEvent} each time a {@link VeniceChangelogConsumer} completes a version swap, and to
 * report the topic of newly subscribed or seeked partitions. See
 * {@link VeniceChangelogConsumer#setVersionSwapListener(VersionSwapListener)} for when and on which thread it is called.
 */
@FunctionalInterface
public interface VersionSwapListener {
  void onVersionSwapCompleted(VersionSwapEvent event);
}
