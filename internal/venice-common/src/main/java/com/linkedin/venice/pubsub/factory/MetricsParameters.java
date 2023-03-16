package com.linkedin.venice.pubsub.factory;

import io.tehuti.metrics.MetricsRepository;


public class MetricsParameters {
  public final String uniqueName;
  public final MetricsRepository metricsRepository;

  public MetricsParameters(String uniqueMetricNamePrefix, MetricsRepository metricsRepository) {
    this.uniqueName = uniqueMetricNamePrefix;
    this.metricsRepository = metricsRepository;
  }

  public MetricsParameters(
      Class pubSubFactoryClass,
      Class usingClass,
      String kafkaBootstrapUrl,
      MetricsRepository metricsRepository) {
    this(
        pubSubFactoryClass.getSimpleName() + "_used_by_" + usingClass + "_for_" + kafkaBootstrapUrl,
        metricsRepository);
  }
}
