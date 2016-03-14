package com.linkedin.venice.meta;

import java.util.Map;


/**
 * Listen on kafka topic and get the notification once the routing data is changed.
 */
public interface RoutingDataChangedListener {
  public void handleRoutingDataCHange(String kafkaTopic, Map<Integer, Partition> partitions);
}
