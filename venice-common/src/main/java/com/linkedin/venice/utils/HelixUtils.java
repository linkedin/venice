package com.linkedin.venice.utils;

import org.apache.helix.api.id.PartitionId;


/**
 * Helper functions for Helix.
 */
public class HelixUtils {

  /**
   * Converts Helix Partition Id to Venice store's partition Id.
   */
  public static int getVenicePartitionIdFromHelixPartitionId(PartitionId partitionId) {
    String helixId = partitionId.stringify();
    int lastUnderscoreIdx = helixId.lastIndexOf('_');
    return Integer.valueOf(helixId.substring(lastUnderscoreIdx+1));
  }

  /**
   * Converts Helix Partition Id to Venice store name.
   */
  public static String getVeniceStoreNameFromHelixPartitionId(PartitionId partitionId) {
    String helixId = partitionId.stringify();
    int lastUnderscoreIdx = helixId.lastIndexOf('_');
    return helixId.substring(0, lastUnderscoreIdx);
  }

  /**
   * Converts the Venice Server Node Id to Helix Participant name.
   */
  public static String convertNodeIdToHelixParticipantName(int nodeId) {
    return "Participant_" + String.valueOf(nodeId);
  }

}
