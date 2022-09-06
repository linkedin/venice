package com.linkedin.venice.meta;

/**
 * Status of the instance in Venice cluster.
 */
public enum InstanceStatus {
  CONNECTED, // Connected to ZK
  DISCONNECTED, // Disconnected from ZK.
}
