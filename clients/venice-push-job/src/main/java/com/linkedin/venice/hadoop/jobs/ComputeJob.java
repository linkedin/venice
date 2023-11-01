package com.linkedin.venice.hadoop.jobs;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.io.IOException;


/**
 * An interface to abstract executing and monitoring a compute job running on any batch compute engine
 */
public interface ComputeJob extends Closeable {
  enum Status {
    NOT_STARTED(0), RUNNING(1), SUCCEEDED(2), FAILED(3), FAILED_VERIFICATION(4), KILLED(5);

    public final int value;
    private static final Status[] values = values();

    Status(int value) {
      this.value = value;
    }

    public static Status valueOf(int value) {
      return values[value];
    }

    public boolean isEnded() {
      return this == SUCCEEDED || this == FAILED || this == FAILED_VERIFICATION || this == KILLED;
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
