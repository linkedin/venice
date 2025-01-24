package com.linkedin.venice.router.api.routing.helix;

/**
 * This simple strategy will try to distribute the load evenly to every Helix Group.
 */
public class HelixGroupRoundRobinStrategy implements HelixGroupSelectionStrategy {
  @Override
  public int selectGroup(long requestId, int groupNum) {
    int assignedGroupId = 0;
    if (groupNum > 0) {
      assignedGroupId = (int) (requestId % groupNum);
    }
    return assignedGroupId;
  }

  @Override
  public void finishRequest(long requestId, int groupId, double latency) {
    // do nothing
  }

}
