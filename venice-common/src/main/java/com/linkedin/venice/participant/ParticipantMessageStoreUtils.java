package com.linkedin.venice.participant;

import com.linkedin.venice.meta.Store;


public class ParticipantMessageStoreUtils {
  private static final String PARTICIPANT_STORE = "participant_store";
  private static final String PARTICIPANT_STORE_FORMAT = Store.SYSTEM_STORE_FORMAT + "_cluster_%s";

  public static String getStoreNameForCluster(String clusterName) {
    return String.format(PARTICIPANT_STORE_FORMAT, PARTICIPANT_STORE, clusterName);
  }
}
