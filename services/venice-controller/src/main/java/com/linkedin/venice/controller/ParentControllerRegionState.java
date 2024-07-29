package com.linkedin.venice.controller;

/**
 * Enum representing the state of the region where the parent controller resides.
 * i.e., Region dc-0 is ACTIVE while Region dc-1 is PASSIVE
 * This means that ParentController in dc-0 is serving requests while ParentController in dc-1 is rejecting requests
 */
public enum ParentControllerRegionState {
  /** The region is active, so the parent controller in the region is serving requests */
  ACTIVE,
  /**
   * The region is passive, so the parent controller in the region is rejecting requests.
   * This region is ready to take over if components in the currently active region fails
   */
  PASSIVE
}
