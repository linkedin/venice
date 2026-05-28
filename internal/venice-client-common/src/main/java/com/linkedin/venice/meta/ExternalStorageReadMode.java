package com.linkedin.venice.meta;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * Store-level read routing for clients backed by both Venice local storage and a configured external storage system.
 * Applies to the store as a whole, not per-version. Mirrors the {@code externalStorageReadMode} field staged on
 * {@code StoreProperties} (StoreMetaValue v44) by PR #2814.
 *
 * <p>Field plumbing only at the OSS Venice layer; the actual routing decisions live in proprietary client code.
 */
public enum ExternalStorageReadMode implements VeniceEnumValue {
  /** Default. All reads served from Venice local storage. No external-storage involvement. */
  VENICE_ONLY(0),
  /**
   * Client reads from both Venice and the external storage in parallel; the external-storage result is compared
   * against the Venice result for correctness metrics but never reaches the user. Used to validate dual-write
   * correctness before cutover.
   */
  DUAL_MODE_CONSISTENCY_CHECK(1),
  /**
   * Client issues reads against both Venice and the external storage in parallel and returns the first response.
   * On miss from the early responder, the slower one is awaited as a fallback. Enabled only after consistency
   * checking has validated zero divergence over a sustained window.
   */
  DUAL_MODE_EARLY_RETURN(2),
  /**
   * All user-data reads served from the external storage. Venice continues to serve metadata (catalog, schemas,
   * compression dictionary) and acts as the metadata/CDC path.
   */
  EXTERNAL_ONLY(3);

  private final int value;
  private static final List<ExternalStorageReadMode> TYPES = EnumUtils.getEnumValuesList(ExternalStorageReadMode.class);

  ExternalStorageReadMode(int value) {
    this.value = value;
  }

  public static ExternalStorageReadMode valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, ExternalStorageReadMode.class);
  }

  @Override
  public int getValue() {
    return value;
  }
}
