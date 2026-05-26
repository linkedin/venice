package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.meta.ExternalStorageReadMode;
import java.util.Objects;


/**
 * Immutable view of the store-level configuration the Fast Client metadata refresh has materialized. Used as the
 * payload of {@link StoreConfigChangeListener} so that consumers can react to runtime changes (e.g. operator flips
 * to {@link ExternalStorageReadMode}) without having to poll {@link StoreMetadata} themselves.
 *
 * <p>Only fields that change at store-granularity (not per-version) belong here. Current-version transitions are
 * delivered by {@link StoreVersionSwitchListener} instead; per-version per-refresh state — partition count, replicas,
 * compression dictionary — is not delivered by either listener today (callers can still poll {@link StoreMetadata}
 * for those).
 *
 * <p>Equality is value-based across all snapshot fields, which is what {@link AbstractStoreMetadata#fireStoreConfigChange}
 * uses to suppress callbacks when the snapshot has not actually changed.
 */
public final class StoreConfigSnapshot {
  private final int batchGetLimit;
  // TODO(external-storage): populate from the metadata response once {@link
  // AvroProtocolDefinition#METADATA_SYSTEM_SCHEMA_STORE}
  // (currently v44) and {@link AvroProtocolDefinition#SERVER_METADATA_RESPONSE} (currently v2) advance to carry
  // externalStorageReadMode on the wire. Until then this is sourced from a callable-provided default.
  private final ExternalStorageReadMode externalStorageReadMode;

  public StoreConfigSnapshot(int batchGetLimit, ExternalStorageReadMode externalStorageReadMode) {
    this.batchGetLimit = batchGetLimit;
    this.externalStorageReadMode =
        externalStorageReadMode == null ? ExternalStorageReadMode.VENICE_ONLY : externalStorageReadMode;
  }

  public int getBatchGetLimit() {
    return batchGetLimit;
  }

  public ExternalStorageReadMode getExternalStorageReadMode() {
    return externalStorageReadMode;
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
    return batchGetLimit == that.batchGetLimit && externalStorageReadMode == that.externalStorageReadMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchGetLimit, externalStorageReadMode);
  }

  @Override
  public String toString() {
    return "StoreConfigSnapshot{batchGetLimit=" + batchGetLimit + ", externalStorageReadMode=" + externalStorageReadMode
        + '}';
  }
}
