package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import java.util.List;
import java.util.Map;


public class AbstractClientRoutingStrategy implements ClientRoutingStrategy {
  @Override
  public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
    throw new VeniceUnsupportedOperationException("getReplicas");
  }

  public void updateHelixGroupInfo(Map<String, Integer> instanceToHelixGroupIdMap) {
    // default implementation is no-op
  }
}
