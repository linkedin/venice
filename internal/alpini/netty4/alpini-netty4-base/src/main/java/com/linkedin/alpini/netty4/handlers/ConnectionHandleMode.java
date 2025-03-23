package com.linkedin.alpini.netty4.handlers;

public enum ConnectionHandleMode {
  /**
   * Fail fast when the connection limit is exceeded.
   * {@link ConnectionLimitHandler} will be added into the pipeline to handle the connection limit.
   */
  FAIL_FAST_WHEN_LIMIT_EXCEEDED,

  /**
   * Stall when the connection limit is exceeded.
   * {@link ConnectionControlHandler} will be added into the pipeline to handle the connection limit.
   */
  STALL_WHEN_LIMIT_EXCEEDED;
}
