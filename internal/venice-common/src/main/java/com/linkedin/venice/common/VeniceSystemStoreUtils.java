package com.linkedin.venice.common;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import java.util.concurrent.TimeUnit;


public class VeniceSystemStoreUtils {
  public static final String PARTICIPANT_STORE = "participant_store";
  public static final String PUSH_JOB_DETAILS_STORE = "push_job_details_store";

  private static final String PARTICIPANT_STORE_PREFIX = String.format(Store.SYSTEM_STORE_FORMAT, PARTICIPANT_STORE);
  private static final String PARTICIPANT_STORE_FORMAT = PARTICIPANT_STORE_PREFIX + "_cluster_%s";
  private static final String PUSH_JOB_DETAILS_STORE_NAME =
      String.format(Store.SYSTEM_STORE_FORMAT, PUSH_JOB_DETAILS_STORE);
  public static final String SEPARATOR = "_";
  public static final UpdateStoreQueryParams DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS =
      new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
          .setHybridOffsetLagThreshold(1)
          .setHybridTimeLagThreshold(-1) // Explicitly disable hybrid time lag measurement on system store
          .setWriteComputationEnabled(true)
          .setPartitionCount(1);

  public static String getParticipantStoreNameForCluster(String clusterName) {
    return String.format(PARTICIPANT_STORE_FORMAT, clusterName);
  }

  public static boolean isParticipantStore(String storeName) {
    if (storeName == null) {
      return false;
    }
    return storeName.startsWith(PARTICIPANT_STORE_PREFIX);
  }

  public static String getPushJobDetailsStoreName() {
    return PUSH_JOB_DETAILS_STORE_NAME;
  }

  public static boolean isSystemStore(String storeName) {
    return storeName.startsWith(Store.SYSTEM_STORE_NAME_PREFIX);
  }

  public static String getDaVinciPushStatusStoreName(String storeName) {
    return VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
  }

  public static String getMetaStoreName(String storeName) {
    return VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
  }

  public static boolean isUserSystemStore(String storeName) {
    VeniceSystemStoreType veniceSystemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    return veniceSystemStoreType != null && veniceSystemStoreType.isStoreZkShared();
  }
}
