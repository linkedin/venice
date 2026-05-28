package com.linkedin.venice.fastclient.meta;

/**
 * Callback notified whenever the Fast Client observes a change to the store-level configuration that it tracks.
 * Fires from within the metadata refresh loop after the new snapshot has been committed to the local cache, so
 * listeners may safely call {@link StoreMetadata} getters and observe the new state.
 *
 * <p>Implementations must be thread-safe and short-running; the metadata refresh thread is shared across all reads
 * for the store. Any exception thrown by a listener is caught and logged, and does not affect other listeners or
 * the metadata refresh itself.
 *
 * <p>Current-version flips are delivered by {@link StoreVersionSwitchListener} (and only that). Partition and
 * replica-routing updates that happen on every metadata refresh, even when the current version does not change, are
 * not delivered by either listener today.
 */
@FunctionalInterface
public interface StoreConfigChangeListener {
  /**
   * @param previous the snapshot observed before this refresh, or {@code null} for the first refresh after client
   *                 start
   * @param current  the snapshot observed by the current refresh; never {@code null}
   */
  void onStoreConfigChange(StoreConfigSnapshot previous, StoreConfigSnapshot current);
}
