package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;


/**
 * Carries the {@link ChunkedValueManifest} of a chunked value back to the caller of a storage lookup.
 *
 * <p>A caller may also declare a ceiling on the assembled value size. When it does, the lookup short-circuits as soon
 * as the manifest is read: the manifest records the fully assembled size, so an oversized value can be identified
 * without fetching and concatenating any of its chunks. The lookup then returns {@code null}, and the caller
 * distinguishes that from a genuinely missing value via {@link #isSizeLimitExceeded()}.
 *
 * <p>This is used by nearline large-record skipping so that a pathological multi-megabyte record is never assembled
 * just to discover that the write must be rejected. Callers that do not declare a ceiling are unaffected.
 */
public class ChunkedValueManifestContainer {
  /** Sentinel meaning the caller accepts any size, which is the default and the behavior for all read paths. */
  public static final int UNLIMITED_SIZE = -1;

  private final int maxAssembledSizeBytes;
  private ChunkedValueManifest manifest;
  private boolean sizeLimitExceeded;

  public ChunkedValueManifestContainer() {
    this(UNLIMITED_SIZE);
  }

  /**
   * @param maxAssembledSizeBytes largest assembled value the caller is willing to have read. Any non-positive value,
   *                              such as {@link #UNLIMITED_SIZE}, imposes no ceiling.
   */
  public ChunkedValueManifestContainer(int maxAssembledSizeBytes) {
    this.maxAssembledSizeBytes = maxAssembledSizeBytes;
  }

  public void setManifest(ChunkedValueManifest manifest) {
    this.manifest = manifest;
    this.sizeLimitExceeded = manifest != null && maxAssembledSizeBytes > 0 && manifest.size > maxAssembledSizeBytes;
  }

  public ChunkedValueManifest getManifest() {
    return manifest;
  }

  /**
   * Whether the value was left unassembled because it exceeds the ceiling this container declared. When this is true, a
   * {@code null} lookup result means "too large to read", not "not found".
   */
  public boolean isSizeLimitExceeded() {
    return sizeLimitExceeded;
  }
}
