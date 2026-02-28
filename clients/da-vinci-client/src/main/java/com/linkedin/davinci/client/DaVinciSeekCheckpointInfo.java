package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.ComplementSet;
import java.util.Map;


public class DaVinciSeekCheckpointInfo {
  private Long allPartitionsTimestamp;
  private Map<Integer, PubSubPosition> postitionMap;
  private Map<Integer, Long> timestampsMap;
  private boolean seekToTail = false;
  private boolean seekToBeginningOfPush = false;

  public DaVinciSeekCheckpointInfo(
      Map<Integer, PubSubPosition> postitionMap,
      Map<Integer, Long> timestampsMap,
      Long allPartitionsTimestamp,
      boolean seekToTail) {
    this(postitionMap, timestampsMap, allPartitionsTimestamp, seekToTail, false);
  }

  public DaVinciSeekCheckpointInfo(
      Map<Integer, PubSubPosition> postitionMap,
      Map<Integer, Long> timestampsMap,
      Long allPartitionsTimestamp,
      boolean seekToTail,
      boolean seekToBeginningOfPush) {
    this.allPartitionsTimestamp = allPartitionsTimestamp;
    this.postitionMap = postitionMap;
    this.timestampsMap = timestampsMap;
    this.seekToTail = seekToTail;
    this.seekToBeginningOfPush = seekToBeginningOfPush;
    int validCheckPointCount = 0;
    if (allPartitionsTimestamp != null) {
      validCheckPointCount++;
    }
    if (seekToTail) {
      validCheckPointCount++;
    }
    if (seekToBeginningOfPush) {
      validCheckPointCount++;
    }
    if (timestampsMap != null) {
      validCheckPointCount++;
    }
    if (postitionMap != null) {
      validCheckPointCount++;
    }
    if (validCheckPointCount > 1) {
      throw new VeniceException("Multiple checkpoint types are not supported");
    }
  }

  public Long getAllPartitionsTimestamp() {
    return allPartitionsTimestamp;
  }

  public Map<Integer, PubSubPosition> getPostitionMap() {
    return postitionMap;
  }

  public void setPositionMap(Map<Integer, PubSubPosition> postitionMap) {
    this.postitionMap = postitionMap;
  }

  public void setTimestampsMap(Map<Integer, Long> timestampsMap) {
    this.timestampsMap = timestampsMap;
  }

  public Map<Integer, Long> getTimestampsMap() {
    return timestampsMap;
  }

  public boolean isSeekToTail() {
    return seekToTail;
  }

  public boolean isSeekToBeginningOfPush() {
    return seekToBeginningOfPush;
  }

  public ComplementSet<Integer> getPartitions() {
    if (postitionMap != null) {
      return ComplementSet.newSet(postitionMap.keySet());
    } else if (timestampsMap != null) {
      return ComplementSet.newSet(timestampsMap.keySet());
    } else {
      return ComplementSet.universalSet();
    }
  }
}
