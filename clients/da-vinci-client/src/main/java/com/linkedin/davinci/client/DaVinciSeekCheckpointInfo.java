package com.linkedin.davinci.client;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.ComplementSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class DaVinciSeekCheckpointInfo {
  public enum SeekMode {
    POSITION_MAP, TIMESTAMPS_MAP
  }

  private final SeekMode seekMode;
  private final Map<Integer, PubSubPosition> positionMap;
  private final Map<Integer, Long> timestampsMap;

  private DaVinciSeekCheckpointInfo(
      SeekMode seekMode,
      Map<Integer, PubSubPosition> positionMap,
      Map<Integer, Long> timestampsMap) {
    this.seekMode = seekMode;
    this.positionMap = positionMap;
    this.timestampsMap = timestampsMap;
  }

  public static DaVinciSeekCheckpointInfo forPositions(Map<Integer, PubSubPosition> positionMap) {
    return new DaVinciSeekCheckpointInfo(
        SeekMode.POSITION_MAP,
        Collections.unmodifiableMap(new HashMap<>(positionMap)),
        null);
  }

  public static DaVinciSeekCheckpointInfo forTimestamps(Map<Integer, Long> timestampsMap) {
    return new DaVinciSeekCheckpointInfo(
        SeekMode.TIMESTAMPS_MAP,
        null,
        Collections.unmodifiableMap(new HashMap<>(timestampsMap)));
  }

  public SeekMode getSeekMode() {
    return seekMode;
  }

  public Map<Integer, PubSubPosition> getPositionMap() {
    return positionMap;
  }

  public Map<Integer, Long> getTimestampsMap() {
    return timestampsMap;
  }

  public ComplementSet<Integer> getPartitions() {
    if (positionMap != null) {
      return ComplementSet.newSet(positionMap.keySet());
    }
    return ComplementSet.newSet(timestampsMap.keySet());
  }
}
