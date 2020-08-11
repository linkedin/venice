package com.linkedin.venice.common;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Time;

import static com.linkedin.venice.common.VeniceSystemStore.*;


public class VeniceSystemStoreUtils {
  public static final String PARTICIPANT_STORE = "participant_store";
  public static final String PUSH_JOB_DETAILS_STORE = "push_job_details_store";

  private static final String PARTICIPANT_STORE_PREFIX = String.format(Store.SYSTEM_STORE_FORMAT, PARTICIPANT_STORE);
  private static final String PARTICIPANT_STORE_FORMAT = PARTICIPANT_STORE_PREFIX + "_cluster_%s";
  private static final String PUSH_JOB_DETAILS_STORE_NAME =
      String.format(Store.SYSTEM_STORE_FORMAT, PUSH_JOB_DETAILS_STORE);
  private static final int DEFAULT_SYSTEM_STORE_PARTITION_COUNT = 1;
  private static final long DEFAULT_SYSTEM_STORE_LAG_THRESHOLD = 100;
  private static final long DEFAULT_SYSTEM_STORE_REWIND_SECONDS = 7 * Time.SECONDS_PER_DAY;
  private static final String SEPARATOR = "_";

  public static String getParticipantStoreNameForCluster(String clusterName) {
    return String.format(PARTICIPANT_STORE_FORMAT, clusterName);
  }

  public static String getPushJobDetailsStoreName() {
    return PUSH_JOB_DETAILS_STORE_NAME;
  }

  public static boolean isSystemStore(String storeName) {
    return storeName.startsWith(Store.SYSTEM_STORE_NAME_PREFIX);
  }

  public static VeniceSystemStore getSystemStoreType(String storeName) {
    if (storeName.startsWith(METADATA_STORE.getPrefix())) {
      return METADATA_STORE;
    } else {
      return null;
    }
  }

  /**
   * @param storeName
   * @return the corresponding store name for shared Zookeeper if applicable. The same store name is returned otherwise.
   */
  public static String getZkStoreName(String storeName) {
    VeniceSystemStore systemStore = getSystemStoreType(storeName);
    return systemStore != null && systemStore.isStoreZkShared() ? systemStore.getPrefix() : storeName;
  }

  public static UpdateStoreQueryParams getDefaultZkSharedStoreParams() {
    UpdateStoreQueryParams defaultParams = new UpdateStoreQueryParams();
    defaultParams
        .setPartitionCount(DEFAULT_SYSTEM_STORE_PARTITION_COUNT)
        .setHybridOffsetLagThreshold(DEFAULT_SYSTEM_STORE_LAG_THRESHOLD)
        .setHybridRewindSeconds(DEFAULT_SYSTEM_STORE_REWIND_SECONDS);
    return defaultParams;
  }

  public static String getMetadataStoreName(String storeName) {
    return METADATA_STORE.getPrefix() + SEPARATOR + storeName;
  }

  public static String getStoreNameFromMetadataStoreName(String metadataStoreName) {
    if (metadataStoreName.isEmpty()) {
      throw new IllegalArgumentException("Empty string is not a valid metadata store name");
    }
    if (!metadataStoreName.startsWith(METADATA_STORE.getPrefix())) {
      throw new IllegalArgumentException("The metadata store name: " + metadataStoreName + " is not valid");
    }
    int index = metadataStoreName.lastIndexOf(METADATA_STORE.getPrefix());
    if (index == -1) {
      throw new IllegalArgumentException("Cannot find the metadata store prefix: " + METADATA_STORE.getPrefix()
          + " in metadata store name: " + metadataStoreName);
    }
    return metadataStoreName.substring(index + METADATA_STORE.getPrefix().length() + SEPARATOR.length());
  }

  public static String getSharedZkNameForMetadataStore(String clusterName) {
    return METADATA_STORE.getPrefix() + SEPARATOR + clusterName;
  }
}
