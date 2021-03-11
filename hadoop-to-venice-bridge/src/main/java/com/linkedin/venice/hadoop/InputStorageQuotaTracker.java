package com.linkedin.venice.hadoop;

import com.linkedin.venice.meta.Store;
import javax.annotation.Nullable;


/**
 * This class is used to keep track of store storage quota and storage overhead ratio and check whether the total
 * input data size exceeds the quota
 */
class InputStorageQuotaTracker {

  private final Long storeStorageQuota;
  private final Double storageEngineOverheadRatio;

  InputStorageQuotaTracker(Long storeStorageQuota, Double storageEngineOverheadRatio) {
    if (storageEngineOverheadRatio != null && storageEngineOverheadRatio == 0) {
      throw new IllegalArgumentException("storageEngineOverheadRatio cannot be zero");
    }
    this.storeStorageQuota = storeStorageQuota;
    this.storageEngineOverheadRatio = storageEngineOverheadRatio;
  }

  boolean exceedQuota(long totalInputStorageSizeInBytes) {
    if (storageEngineOverheadRatio == null
        || storeStorageQuota == null
        || storeStorageQuota == Store.UNLIMITED_STORAGE_QUOTA
    ) {
      return false;
    }
    final long veniceDiskUsageEstimate = (long) (totalInputStorageSizeInBytes / storageEngineOverheadRatio);
    return veniceDiskUsageEstimate > storeStorageQuota;
  }

  @Nullable
  Long getStoreStorageQuota() {
    return storeStorageQuota;
  }

  @Nullable
  Double getStorageEngineOverheadRatio() {
    return storageEngineOverheadRatio;
  }
}
