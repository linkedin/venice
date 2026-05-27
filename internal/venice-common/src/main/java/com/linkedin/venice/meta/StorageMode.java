package com.linkedin.venice.meta;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * Per-version storage mode controlling where data for a Venice store version is persisted relative to the optional
 * external storage system. Mirrors the {@code storageMode} field staged on {@code StoreVersion} (StoreMetaValue v44)
 * by PR #2814.
 *
 * <p>Read-side routing is selected separately by {@link ExternalStorageReadMode} at the store level.
 */
public enum StorageMode implements VeniceEnumValue {
  /** Default. Data persisted in Venice local storage only. Current behavior. */
  INTERNAL(0),
  /**
   * Data is written to both Venice local storage and the configured external storage. The specific dual-write
   * implementation — leader-consumer-pipeline vs Venice Push Job — is selected by separate server/VPJ configuration,
   * not by this enum.
   */
  DUAL_WRITE(1),
  /**
   * External-storage-only. Venice's data partition becomes NoOp while metadata partitions are still persisted
   * locally for checkpointing.
   */
  EXTERNAL(2);

  private final int value;
  private static final List<StorageMode> TYPES = EnumUtils.getEnumValuesList(StorageMode.class);

  StorageMode(int value) {
    this.value = value;
  }

  public static StorageMode valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, StorageMode.class);
  }

  @Override
  public int getValue() {
    return value;
  }
}
