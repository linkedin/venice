package com.linkedin.venice.fastclient.meta;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class HelixGroupInfo {
  private final Map<String, Integer> helixGroupInfo;
  private final List<Integer> groupIds;

  public HelixGroupInfo(Map<String, Integer> helixGroupInfo) {
    this.helixGroupInfo = helixGroupInfo;
    this.groupIds = helixGroupInfo.values().stream().distinct().sorted().collect(Collectors.toList());
  }

  public Map<String, Integer> getHelixGroupInfoMap() {
    return helixGroupInfo;
  }

  public List<Integer> getGroupIds() {
    return groupIds;
  }
}
