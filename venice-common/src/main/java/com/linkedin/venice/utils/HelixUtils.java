package com.linkedin.venice.utils;



/**
 * Helper functions for Helix.
 */
public class HelixUtils {

  /**
   * Converts the Venice Server Node Id to Helix Participant name.
   */
  public static String convertNodeIdToHelixParticipantName(int nodeId) {
    return "Participant_" + String.valueOf(nodeId);
  }

  private final static Character SEPARATOR = '_';
  public static int getPartitionId(String helixPartitionName) {
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if(lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException(" Incorrect Helix Partition Name " + helixPartitionName);
    }
    return Integer.valueOf(helixPartitionName.substring(lastUnderscoreIdx+1));
  }

  public static String getStoreName(String helixPartitionName) {
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if(lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException(" Incorrect Helix Partition Name " + helixPartitionName);
    }
    String storeName = helixPartitionName.substring(0, lastUnderscoreIdx);
    if(storeName == null || storeName.length() == 0) {
       throw new IllegalArgumentException(" Could not determine store name from Helix Partition Id " + helixPartitionName);
    }
    return storeName;
  }

}
