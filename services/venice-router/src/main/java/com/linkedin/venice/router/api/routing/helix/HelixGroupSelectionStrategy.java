package com.linkedin.venice.router.api.routing.helix;

public interface HelixGroupSelectionStrategy {
  /**
   * Select a Helix Group for the current request.
   */
  int selectGroup(long requestId, int groupCount);

  /**
   * Notify the corresponding Helix Group that the request is completed, and the implementation will decide whether
   * any cleanup is required or not.
   */
  void finishRequest(long requestId, int groupId);

  /**
   * Get the maximum of the pending requests among all the groups
   */
  int getMaxGroupPendingRequest();

  /**
   * Get the minimum of the pending requests among all the groups
   */
  int getMinGroupPendingRequest();

  /**
   * Get the average of the pending requests among all the groups
   */
  int getAvgGroupPendingRequest();
}
