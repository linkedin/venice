package com.linkedin.venice.jobs;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.io.IOException;


/**
 * An interface to abstract executing and monitoring a compute job running on any batch compute engine
 */
public interface ComputeJob extends Closeable {
  enum Status {
    /** A job that has not started execution yet */
    NOT_STARTED(false),
    /** A job that is currently executing */
    RUNNING(false),
    /** A job that has completed execution successfully */
    SUCCEEDED(true),
    /** A job that failed during it's execution */
    FAILED(true),
    /** A job that has completed execution but failed verification */
    FAILED_VERIFICATION(true),
    /** A job that was killed */
    KILLED(true);

    private final boolean isTerminal;

    Status(boolean terminal) {
      this.isTerminal = terminal;
    }

    public boolean isTerminal() {
      return isTerminal;
    }
  }

  void configure(VeniceProperties properties);

  void runJob();

  Status getStatus();

  Throwable getFailureReason();

  default void kill() {
  }

  @Override
  default void close() throws IOException {
    // Do nothing
  }
}
