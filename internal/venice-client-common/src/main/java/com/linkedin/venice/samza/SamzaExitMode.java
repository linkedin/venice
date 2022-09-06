package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Various methods for stopping/exiting a Samza task.
 */
public enum SamzaExitMode {
  /**
   * Directly exit the Samza process with status code 0, which should be only used by stream reprocessing job;
   * besides, status 0 will leave a green mark for Samza task, so this mode should be used when the stream reprocessing
   * job succeeds.
   */
  SUCCESS_EXIT(0),

  /**
   * Stop the Samza task by keep throwing exception.
   */
  THROW_ERROR_EXCEPTION(1),

  /**
   * Do nothing; this mode is mostly used in test cases.
   */
  NO_OP(2);

  private final int value;

  SamzaExitMode(int value) {
    this.value = value;
  }

  public void exit() {
    switch (this) {
      case SUCCESS_EXIT:
        System.exit(0);
      case THROW_ERROR_EXCEPTION:
        throw new VeniceException("Samza stream reprocessing job is in error state.");
      case NO_OP:
        return;
      default:
        throw new VeniceException("Unknown Samza stream reprocessing exit mode: " + this.name());
    }
  }
}
