package com.linkedin.venice.client.store;

public interface ClientCallback {
  void executeOnSuccess();

  void executeOnError();
}
