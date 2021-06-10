package com.linkedin.venice.common;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Time;

import static com.linkedin.venice.common.VeniceSystemStoreType.*;


public class VeniceSystemStoreUtils {
  public static final String PARTICIPANT_STORE = "participant_store";
  public static final String PUSH_JOB_DETAILS_STORE = "push_job_details_store";

  public static final int DEFAULT_SYSTEM_STORE_PARTITION_COUNT = 1;
  public static final long DEFAULT_SYSTEM_STORE_LAG_THRESHOLD = 100;
  public static final long DEFAULT_SYSTEM_STORE_REWIND_SECONDS = 7 * Time.SECONDS_PER_DAY;

  private static final String PARTICIPANT_STORE_PREFIX = String.format(Store.SYSTEM_STORE_FORMAT, PARTICIPANT_STORE);
  private static final String PARTICIPANT_STORE_FORMAT = PARTICIPANT_STORE_PREFIX + "_cluster_%s";
  private static final String PUSH_JOB_DETAILS_STORE_NAME =
      String.format(Store.SYSTEM_STORE_FORMAT, PUSH_JOB_DETAILS_STORE);
  public static final String SEPARATOR = "_";

  public static String getParticipantStoreNameForCluster(String clusterName) {
    return String.format(PARTICIPANT_STORE_FORMAT, clusterName);
  }

  public static String getPushJobDetailsStoreName() {
    return PUSH_JOB_DETAILS_STORE_NAME;
  }

  public static boolean isSystemStore(String storeName) {
    return storeName.startsWith(Store.SYSTEM_STORE_NAME_PREFIX);
  }

  public static boolean isStoreHostingSharedMetadata(String clusterName, String storeName) {
    return VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(clusterName).equals(storeName);
  }

  public static VeniceSystemStoreType getSystemStoreType(String storeName) {
    if (storeName.startsWith(METADATA_STORE.getPrefix())) {
      return METADATA_STORE;
    } else if (storeName.startsWith(DAVINCI_PUSH_STATUS_STORE.getPrefix())) {
      return DAVINCI_PUSH_STATUS_STORE;
    } else {
      return null;
    }
  }

  /**
   * @param storeName
   * @return the corresponding store name for shared Zookeeper if applicable. The same store name is returned otherwise.
   */
  public static String getZkStoreName(String storeName) {
    VeniceSystemStoreType systemStore = getSystemStoreType(storeName);
    return systemStore != null && systemStore.isStoreZkShared() ? systemStore.getZkSharedStoreName() : storeName;
  }

  public static UpdateStoreQueryParams getDefaultZkSharedStoreParams() {
    UpdateStoreQueryParams defaultParams = new UpdateStoreQueryParams();
    defaultParams
        .setPartitionCount(DEFAULT_SYSTEM_STORE_PARTITION_COUNT)
        .setHybridOffsetLagThreshold(DEFAULT_SYSTEM_STORE_LAG_THRESHOLD)
        .setHybridRewindSeconds(DEFAULT_SYSTEM_STORE_REWIND_SECONDS);
    return defaultParams;
  }

  public static String getSystemStoreName(String storeName, VeniceSystemStoreType storeType) {
    return storeType.getPrefix() + SEPARATOR + storeName;
  }

  public static String getMetadataStoreName(String storeName) {
    return getSystemStoreName(storeName, METADATA_STORE);
  }

  public static String getDaVinciPushStatusStoreName(String storeName) {
    return getSystemStoreName(storeName, DAVINCI_PUSH_STATUS_STORE);
  }

  public static String getStoreNameFromSystemStoreName(String storeName) {
    if (storeName.isEmpty()) {
      throw new IllegalArgumentException("Empty string is not a valid push status store name");
    }
    VeniceSystemStoreType storeType = getSystemStoreType(storeName);
    if (storeType == null) {
      return storeName;
    }
    int index = storeName.lastIndexOf(storeType.getPrefix());
    if (index == -1) {
      throw new IllegalArgumentException("Cannot find the store prefix: " + storeType.getPrefix()
          + " in store name: " + storeName);
    }
    return storeName.substring(index + storeType.getPrefix().length() + SEPARATOR.length());
  }

  public static String getSharedZkNameForMetadataStore(String clusterName) {
    return METADATA_STORE.getPrefix() + SEPARATOR + clusterName;
  }

}
