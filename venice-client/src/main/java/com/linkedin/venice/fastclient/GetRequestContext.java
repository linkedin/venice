package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class GetRequestContext extends RequestContext {
  int partitionId = -1;
  boolean noAvailableReplica = false;

  double decompressionTime = -1;
  double responseDeserializationTime = -1;
  double requestSerializationTime = -1;
  double requestSubmissionToResponseHandlingTime = -1;

  long requestSentTimestampNS = -1;
  Map<String, CompletableFuture<HttpStatus>> replicaRequestMap = new VeniceConcurrentHashMap<>();
  InstanceHealthMonitor instanceHealthMonitor = null;

}
