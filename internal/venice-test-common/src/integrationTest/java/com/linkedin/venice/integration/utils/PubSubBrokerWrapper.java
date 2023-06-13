package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.utils.SslUtils.VeniceTlsConfiguration;

import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import java.io.File;


public abstract class PubSubBrokerWrapper extends ProcessWrapper {
  PubSubBrokerWrapper(String serviceName, File dataDirectory) {
    super(serviceName, dataDirectory);
  }

  public abstract int getSslPort();

  public String getSSLAddress() {
    return getHost() + ":" + getSslPort();
  }

  public abstract VeniceTlsConfiguration getTlsConfiguration();

  @Override
  public String toString() {
    return "PubSubService[" + getServiceName() + "@" + getHost() + ":" + getPort() + "/(ssl)" + getSslPort() + "]";
  }

  public abstract PubSubClientsFactory getPubSubClientsFactory();
}
