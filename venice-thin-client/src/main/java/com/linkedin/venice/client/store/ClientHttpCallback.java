package com.linkedin.venice.client.store;

import com.linkedin.venice.exceptions.VeniceException;

/**
 * Venice client request callback. This is for custom callback.
 * Right now, it is mainly used for metrics recording.
 */
public interface ClientHttpCallback extends ClientCallback{
  //void executeOnSuccess();

  void executeOnError(int httpStatus);

  //void executeOnError();
}
