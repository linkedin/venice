package com.linkedin.venice.fastclient.meta;

/**
 * Different routing strategy types for client side routing:
 *
 * 1. LEAST_LOADED: select replicas based on the least number of pending requests from the local client's perspective.
 * 2. HELIX_ASSISTED: select replicas prioritizing using hosts from the same helix/zone group to minimize request blast
 *    radius for batch gets.
 */
public enum ClientRoutingStrategyType {
  LEAST_LOADED, HELIX_ASSISTED
}
