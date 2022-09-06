package com.linkedin.venice.client.exceptions;

public class ServiceDiscoveryException extends VeniceClientException {
  public ServiceDiscoveryException(String msg, Throwable e) {
    super(msg, e);
  }

  public ServiceDiscoveryException(Throwable e) {
    super(e);
  }

  public ServiceDiscoveryException(String msg) {
    super(msg);
  }
}
