package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.grpc.GrpcRequestContext;


public class VeniceServerGrpcRequestProcessor {
  private VeniceServerGrpcHandler head = null;

  public VeniceServerGrpcRequestProcessor() {
  }

  public void addHandler(VeniceServerGrpcHandler handler) {
    if (head == null) {
      head = handler;
      return;
    }

    VeniceServerGrpcHandler current = head;
    while (current.getNext() != null) {
      current = current.getNext();
    }

    current.addNextHandler(handler);
  }

  public void process(GrpcRequestContext ctx) {
    head.processRequest(ctx);
  }
}
