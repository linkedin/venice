package com.linkedin.venice.hadoop;

import com.linkedin.venice.meta.Store;
import javax.annotation.Nullable;


/**
 * This class is used to keep track of store storage quota and storage overhead ratio and check whether the total
 * input data size exceeds the quota
 */
public class InputStorageQuotaTracker {
  private final Long storeStorageQuota;

  public InputStorageQuotaTracker(Long storeStorageQuota) {
    this.storeStorageQuota = storeStorageQuota;
  }

  public boolean exceedQuota(long totalInputStorageSizeInBytes) {
    if (storeStorageQuota == null || storeStorageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      return false;
    }
    return totalInputStorageSizeInBytes > storeStorageQuota;
  }

  @Nullable
  public Long getStoreStorageQuota() {
    return storeStorageQuota;
  }
}
