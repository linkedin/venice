package com.linkedin.venice.hooks;

/**
 * A hook which returns this enum has the option of proceeding, aborting, waiting or rolling back.
 */
public enum StoreVersionLifecycleEventOutcome {
  /** Proceed, as if the hook did not exist. */
  PROCEED,

  /** Fail the push job associated with this hook. */
  ABORT,

  /** Re-invoke the hook again some time in the future. */
  WAIT,

  /**
   * Rollback to the store-version which was in effect prior to the start of the push job (i.e. other regions which
   * already proceeded will kill the ongoing push if it is still in process, or rollback to the previous store-version,
   * if the push already completed in that region).
   */
  ROLLBACK;
}
