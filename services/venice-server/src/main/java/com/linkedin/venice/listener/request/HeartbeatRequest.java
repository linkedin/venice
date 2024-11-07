package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;


public class HeartbeatRequest {
  private final String topic;
  private final int partition;
  private final boolean filterLagReplica;

  private HeartbeatRequest(String topic, int partition, boolean filterLagReplica) {
    this.topic = topic;
    this.partition = partition;
    this.filterLagReplica = filterLagReplica;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public boolean isFilterLagReplica() {
    return filterLagReplica;
  }

  public static HeartbeatRequest parseGetHttpRequest(String uri, String[] requestParts) {
    if (requestParts.length == 5) {
      // [0]""/[1]"action"/[2]"topic"/[3]"partition"/[4]"filter lagging flag"
      try {
        String topic = requestParts[2];
        int partition = Integer.parseInt(requestParts[3]);
        boolean filterLagReplica = Boolean.parseBoolean(requestParts[4]);
        return new HeartbeatRequest(topic, partition, filterLagReplica);
      } catch (Exception e) {
        throw new VeniceException("Unable to parse request for a HeartbeatRequest action: " + uri);
      }
    } else {
      throw new VeniceException("not a valid request for a HeartbeatRequest action: " + uri);
    }
  }

}
