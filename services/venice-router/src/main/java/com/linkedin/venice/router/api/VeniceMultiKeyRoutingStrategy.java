package com.linkedin.venice.router.api;

/**
 * This enum is used to define various routing strategies for multi-key requests.
 */
public enum VeniceMultiKeyRoutingStrategy {
  // This mode will send the request to the first host of all the available replicas
  GROUP_BY_PRIMARY_HOST_ROUTING,
  // This mode will try send minimum of requests to storage node.
  GREEDY_ROUTING,
  // This mode will send the request to the least loaded host of all the available replicas.
  LEAST_LOADED_ROUTING,
  // This mode will try to limit the fanout inside one helix group/zone.
  HELIX_ASSISTED_ROUTING
}
