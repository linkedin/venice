package com.linkedin.venice.controllerapi;

public enum NodeReplicasReadinessState {
  /** Node is not live. */
  INANIMATE,

  /** Node is live and all current version replicas are ready. */
  READY,

  /** Node is live but not all current version replicas are ready. */
  UNREADY
}
