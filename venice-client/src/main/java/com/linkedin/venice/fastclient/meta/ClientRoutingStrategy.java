package com.linkedin.venice.fastclient.meta;

import java.util.List;


public interface ClientRoutingStrategy {
  List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount);

}
