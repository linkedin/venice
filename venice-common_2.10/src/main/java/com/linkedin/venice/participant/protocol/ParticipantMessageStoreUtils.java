package com.linkedin.venice.participant.protocol;

public class ParticipantMessageStoreUtils {
  private static final String STORE_NAME_FORMAT = "__participant_message_store_cluster_%s__";

  public static String getStoreNameForCluster(String clusterName) {
    return String.format(STORE_NAME_FORMAT, clusterName);
  }
}
