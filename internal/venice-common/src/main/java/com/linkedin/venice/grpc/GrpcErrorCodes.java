package com.linkedin.venice.grpc;

public class GrpcErrorCodes {
  /**
   * We want to define our own error codes/messages for when we receive any sort of exception or error when processing
   * a gRPC request to remain flexible and not bound to the gRPC error codes. We define some constants here to be used
   * within our gRPC service and handlers and our gRPC client is responsible for mapping the gRPC error code to the
   * proper Venice Exception.
   */

  public static final int DEADLINE_EXCEEDED = 100;
  public static final int KEY_NOT_FOUND = 101;

  public static final int OK = 200;
  public static final int BAD_REQUEST = 400;
  public static final int INTERNAL_ERROR = 500;
  public static final int TOO_MANY_REQUESTS = 501;
  public static final int SERVICE_UNAVAILABLE = 503;
}
