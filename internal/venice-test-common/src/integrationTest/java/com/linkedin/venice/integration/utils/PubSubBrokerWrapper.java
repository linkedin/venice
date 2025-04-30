package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.utils.SslUtils.VeniceTlsConfiguration;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
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

  public Map<String, String> getAdditionalConfig() {
    return Collections.emptyMap();
  }

  /**
   * Returns a map of configs that should be merged with other brokers configs to form a single value.
   */
  public Map<String, String> getMergeableConfigs() {
    return Collections.emptyMap();
  }

  public abstract PubSubPositionTypeRegistry getPubSubPositionTypeRegistry();

  /**
   * Returns a map of broker details for clients to connect to the broker.
   *
   * The values of common key in getMergeableConfigs() will be merged to form a single value.
   *
   * @param pubSubBrokerWrappers List of PubSubBrokerWrapper
   *                             (e.g. KafkaBrokerWrapper, PulsarBrokerWrapper, etc.)
   * @return Map of broker details for clients to connect to the broker.
   */
  public static Map<String, String> getBrokerDetailsForClients(List<PubSubBrokerWrapper> pubSubBrokerWrappers) {
    Map<String, String> configs = new HashMap<>();
    pubSubBrokerWrappers.forEach(pubSubBrokerWrapper -> configs.putAll(pubSubBrokerWrapper.getAdditionalConfig()));
    List<Map<String, String>> toBeMergedList =
        pubSubBrokerWrappers.stream().map(PubSubBrokerWrapper::getMergeableConfigs).collect(Collectors.toList());
    configs.putAll(TestUtils.mergeConfigs(toBeMergedList));

    return configs;
  }
}
