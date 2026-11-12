package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.fastclient.RequestContext;
import java.util.List;
import java.util.Map;


public class AbstractClientRoutingStrategy {
  public String getReplicas(long requestId, int groupId, List<String> replicas) {
    throw new VeniceUnsupportedOperationException("getReplicas");
  }

  public void updateHelixGroupInfo(Map<String, Integer> instanceToHelixGroupIdMap) {
    // default implementation is no-op
  }

  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest) {
    return -1;
  }

  /**
   * Size-aware group selection: {@code keyCount} is the number of keys in the request, letting a strategy route by
   * request size. Defaults to the size-agnostic {@link #getHelixGroupId(long, int)} so strategies that do not care about
   * request size need no change.
   */
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest, int keyCount) {
    return getHelixGroupId(requestId, groupIdForOriginalRequest);
  }

  /**
   * This method is used to track the request for any custom logic that needs to be executed when a request is sent.
   *
   * @return boolean to indicate whether the request is tracked or not.
   */
  public boolean trackRequest(RequestContext requestContext) {
    // Do nothing by default
    return false;
  }
}
