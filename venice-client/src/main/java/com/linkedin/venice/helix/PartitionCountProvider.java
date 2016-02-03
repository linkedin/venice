package com.linkedin.venice.helix;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;


/**
 * Based on org.apache.helix.spectator.RoutingTableProvider
 */
public class PartitionCountProvider implements ExternalViewChangeListener {
  private static final Logger logger = Logger.getLogger(PartitionCountProvider.class);
  private Map<String, Integer> storeToPartitionCount = new ConcurrentHashMap<>();

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    for (ExternalView view : externalViewList){
      String resourceName = view.getResourceName();
      int partitionCount = view.getPartitionSet().size();
      storeToPartitionCount.put(resourceName, partitionCount);
    }
  }

  public int getPartitionsForStore(String storeName){
    if (storeToPartitionCount.containsKey(storeName)) {
      return storeToPartitionCount.get(storeName);
    } else {
      String errMsg = "External View does not contain partitions for store: " + storeName;
      logger.error(errMsg);
      logger.error("Current store to partition count:");
      for (String store : storeToPartitionCount.keySet()){
        logger.error(store + ": " + storeToPartitionCount.get(store));
      }
      throw new IllegalArgumentException(errMsg);
    }
  }
}
