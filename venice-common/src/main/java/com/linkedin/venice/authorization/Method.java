package com.linkedin.venice.authorization;

/**
 * Collection of all available methods that a {@link Principal} can perform.
 */
public enum Method {
  POST,
  PUT,
  GET,
  DELETE,
  HEAD,
  PATCH,
  TRACE,
  OPTIONS,
  GET_ALL,
  BATCH_GET,
  BATCH_CREATE,
  BATCH_UPDATE,
  PARTIAL_UPDATE,
  BATCH_PARTIAL_UPDATE,
  BATCH_DELETE,

  Read,
  Write,

  REQUEST_TOPIC,
  ADD_VERSION,
  END_OF_PUSH,
  OFFLINE_PUSH_INFO,
  ADD_VALUE_SCHEMA,
  KILL_OFFLINE_PUSH_JOB,

  UNKNOWN
}
