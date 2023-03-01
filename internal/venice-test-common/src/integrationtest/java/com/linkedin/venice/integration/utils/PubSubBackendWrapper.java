package com.linkedin.venice.integration.utils;

import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import java.io.File;


public abstract class PubSubBackendWrapper extends ProcessWrapper {
  PubSubBackendWrapper(String serviceName, File dataDirectory) {
    super(serviceName, dataDirectory);
  }

  abstract int getSslPort();

  String getSSLAddress() {
    return getHost() + ":" + getSslPort();
  }

  @Override
  public String toString() {
    return "PubSubService[" + getServiceName() + "@" + getHost() + ":" + getPort() + "/" + getSslPort() + "]";
  }

  abstract PubSubClientsFactory getPubSubClientsFactory();
}
