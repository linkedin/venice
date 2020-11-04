package com.linkedin.venice.fastclient;

import java.util.concurrent.atomic.AtomicLong;


/**
 * This class is used to include all the intermediate fields required for the communication between the different tiers.
 */
public class RequestContext {
  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong();

  int currentVersion = -1;
  final long requestId;

  public RequestContext() {
    this.requestId = REQUEST_ID_GENERATOR.getAndIncrement();
  }

}
