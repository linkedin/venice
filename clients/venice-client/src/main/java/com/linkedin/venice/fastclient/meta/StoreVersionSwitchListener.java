package com.linkedin.venice.fastclient.meta;

/**
 * Callback notified whenever the Fast Client observes a change in a store's current serving version. Fires from
 * within the metadata refresh loop after the new version has been committed to the local cache, so listeners may
 * safely call {@link StoreMetadata#getCurrentStoreVersion()} and observe the new value.
 *
 * <p>Implementations must be thread-safe and short-running; the metadata refresh thread is shared across all reads
 * for the store. Any exception thrown by a listener is caught and logged, and does not affect other listeners or
 * the metadata refresh itself.
 */
@FunctionalInterface
public interface StoreVersionSwitchListener {
  /**
   * @param previousVersion the version that was current before this refresh, or {@code -1} for the first refresh
   *                        after client start
   * @param newVersion      the new current version observed by the metadata refresh; always {@code > 0}
   */
  void onVersionSwitch(int previousVersion, int newVersion);
}
