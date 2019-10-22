package com.linkedin.venice.common;

import com.linkedin.venice.meta.Store;


public class VeniceSystemStoreUtils {
  private static final String PARTICIPANT_STORE = "participant_store";
  private static final String PUSH_JOB_DETAILS_STORE = "push_job_details_store";
  private static final String PARTICIPANT_STORE_FORMAT = Store.SYSTEM_STORE_FORMAT + "_cluster_%s";
  private static final String PUSH_JOB_DETAILS_STORE_NAME =
      String.format(Store.SYSTEM_STORE_FORMAT, PUSH_JOB_DETAILS_STORE);

  public static String getParticipantStoreNameForCluster(String clusterName) {
    return String.format(PARTICIPANT_STORE_FORMAT, PARTICIPANT_STORE, clusterName);
  }

  public static String getPushJobDetailsStoreName() {
    return PUSH_JOB_DETAILS_STORE_NAME;
  }
}
