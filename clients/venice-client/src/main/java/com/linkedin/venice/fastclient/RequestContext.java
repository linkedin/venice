package com.linkedin.venice.fastclient;

import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This class is used to include all the intermediate fields required for the communication between the different tiers.
 */
public abstract class RequestContext {
  int currentVersion = -1;
  boolean noAvailableReplica = false;

  double decompressionTime = -1;
  double responseDeserializationTime = -1;
  double requestSerializationTime = -1;
  double requestSubmissionToResponseHandlingTime = -1;

  long requestSentTimestampNS = -1;

  // Keeping track for successful keys for the request.
  AtomicInteger successRequestKeyCount = new AtomicInteger(0);

  InstanceHealthMonitor instanceHealthMonitor = null;

  Map<String, CompletableFuture<Integer>> routeRequestMap = new VeniceConcurrentHashMap<>();

  String serverClusterName;

  public RequestContext() {
  }

  public abstract RequestType getRequestType();
}
