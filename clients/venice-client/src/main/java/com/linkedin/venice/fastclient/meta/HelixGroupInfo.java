package com.linkedin.venice.fastclient.meta;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class HelixGroupInfo {
  private final Map<String, Integer> instanceToHelixGroupIdMap;
  private final List<Integer> groupIds;

  public HelixGroupInfo(Map<String, Integer> instanceToHelixGroupIdMap) {
    this.instanceToHelixGroupIdMap = instanceToHelixGroupIdMap;
    this.groupIds = instanceToHelixGroupIdMap.values().stream().distinct().sorted().collect(Collectors.toList());
  }

  public Map<String, Integer> getHelixGroupInfoMap() {
    return instanceToHelixGroupIdMap;
  }

  public List<Integer> getGroupIds() {
    return groupIds;
  }
}
