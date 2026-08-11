package com.linkedin.venice.client.store.listeners;

import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.StorageMode;
import java.util.Objects;


/**
 * Immutable view of the store-level configuration the Fast Client metadata refresh has materialized. Used as the
 * payload of {@link StoreConfigChangeListener} so that consumers can react to runtime changes (e.g. operator flips
 * to {@link ExternalStorageReadMode}) without having to poll {@link StoreMetadata} themselves.
 *
 * <p>Only fields that change at store-granularity (not per-version) belong here, with one deliberate exception:
 * {@link #getCurrentVersionStorageMode()}, which callers must observe together with
 * {@link #getExternalStorageReadMode()} as of the same refresh to gate external-storage reads correctly.
 * Current-version <em>number</em> transitions are delivered by {@link StoreVersionSwitchListener} instead;
 * per-version per-refresh state — partition count, replicas, compression dictionary — is not delivered by either
 * listener today (callers can still poll {@link StoreMetadata} for those).
 *
 */
public final class StoreConfigSnapshot {
  private final int batchGetLimit;
  private final ExternalStorageReadMode externalStorageReadMode;
  private final StorageMode currentVersionStorageMode;

  /**
   * @deprecated use {@link #StoreConfigSnapshot(int, ExternalStorageReadMode, StorageMode)}. Retained for source and
   * binary compatibility; defaults {@code currentVersionStorageMode} to {@link StorageMode#INTERNAL}.
   */
  @Deprecated
  public StoreConfigSnapshot(int batchGetLimit, ExternalStorageReadMode externalStorageReadMode) {
    this(batchGetLimit, externalStorageReadMode, StorageMode.INTERNAL);
  }

  public StoreConfigSnapshot(
      int batchGetLimit,
      ExternalStorageReadMode externalStorageReadMode,
      StorageMode currentVersionStorageMode) {
    this.batchGetLimit = batchGetLimit;
    this.externalStorageReadMode =
        externalStorageReadMode == null ? ExternalStorageReadMode.VENICE_ONLY : externalStorageReadMode;
    this.currentVersionStorageMode =
        currentVersionStorageMode == null ? StorageMode.INTERNAL : currentVersionStorageMode;
  }

  public int getBatchGetLimit() {
    return batchGetLimit;
  }

  public ExternalStorageReadMode getExternalStorageReadMode() {
    return externalStorageReadMode;
  }

  /**
   * @return the storage mode of the store's current serving version as of this snapshot's metadata refresh.
   * Defaults to {@link StorageMode#INTERNAL} when the server does not report it, keeping external-storage reads
   * gated off until the storage mode is known.
   */
  public StorageMode getCurrentVersionStorageMode() {
    return currentVersionStorageMode;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StoreConfigSnapshot)) {
      return false;
    }
    StoreConfigSnapshot that = (StoreConfigSnapshot) other;
    return batchGetLimit == that.batchGetLimit && externalStorageReadMode == that.externalStorageReadMode
        && currentVersionStorageMode == that.currentVersionStorageMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchGetLimit, externalStorageReadMode, currentVersionStorageMode);
  }

  @Override
  public String toString() {
    return "StoreConfigSnapshot{batchGetLimit=" + batchGetLimit + ", externalStorageReadMode=" + externalStorageReadMode
        + ", currentVersionStorageMode=" + currentVersionStorageMode + '}';
  }
}
