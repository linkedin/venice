package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;


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

  public static <T> void updateToHelix(ZkBaseDataAccessor<T> dataAccessor, String path, T data, int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.set(path, data, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new VeniceException("Can not update data to Helix after " + retryCount + " times retry");
  }

  //TODO there is not atomic operations to update multiple node to ZK. We should ask Helix library to rollback if it's only partial successful.
  public static <T> void updateChildrenToHelix(ZkBaseDataAccessor<T> dataAccessor, List<String> pathes, List<T> data,
      int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      boolean[] results = dataAccessor.setChildren(pathes, data, AccessOption.PERSISTENT);
      boolean isAllSuccessful = true;
      for (Boolean r : results) {
        if (!r) {
          isAllSuccessful = false;
          break;
        }
      }
      if (isAllSuccessful) {
        return;
      }
      retry++;
      if (retry == retryCount) {
        throw new VeniceException("Can not update data to Helix after " + retryCount + " times retry");
      }
    }
  }

  public static <T> void removeFromHelix(ZkBaseDataAccessor<T> dataAccessor, String path, int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.remove(path, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new VeniceException("Can not remove data from Helix after " + retryCount + " times retry");
  }
}
