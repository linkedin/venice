package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.utils.SslUtils.VeniceTlsConfiguration;

import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


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

  public abstract String getPubSubClusterName();

  public abstract String getRegionName();

  Map<String, String> getAdditionalConfig() {
    return Collections.emptyMap();
  }

  /**
   * Configs that have the same key, will be merged into a single config with individual values separated by commas.
   */
  public static Map<String, String> combineAdditionalConfigs(List<PubSubBrokerWrapper> pubSubBrokerWrappers) {
    List<Map<String, String>> additionalConfigs =
        pubSubBrokerWrappers.stream().map(PubSubBrokerWrapper::getAdditionalConfig).collect(Collectors.toList());
    return TestUtils.combineConfigs(additionalConfigs);
  }
}
