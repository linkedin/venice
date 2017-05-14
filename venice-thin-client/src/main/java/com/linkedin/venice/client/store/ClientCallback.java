package com.linkedin.venice.client.store;

import com.linkedin.venice.exceptions.VeniceException;

/**
 * Venice client request callback. This is for custom callback.
 * Right now, it is mainly used for metrics recording.
 */
public interface ClientCallback {
  void executeOnSuccess();

  default void executeOnError(int httpStatus) {
    throw new VeniceException("Code has not been implemented");
  }

  void executeOnError();
}
