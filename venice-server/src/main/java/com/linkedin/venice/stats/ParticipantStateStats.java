package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;

import static com.linkedin.venice.helix.VenicePartitionStateModel.*;

public class ParticipantStateStats extends AbstractVeniceStats  {
  private Sensor fromOfflineToBootstrapPartitionNumberSensor;
  private Sensor fromBootstrapToOnlinePartitionNumberSensor;

  public ParticipantStateStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    fromOfflineToBootstrapPartitionNumberSensor = registerSensor("number_of_partitions_from_offline_to_bootstrap", new Gauge(() -> getNumberOfPartitionsFromOfflineToBootstrap()));
    fromBootstrapToOnlinePartitionNumberSensor = registerSensor("number_of_partitions_from_bootstrap_to_online", new Gauge(() -> getNumberOfPartitionsFromBootstrapToOnline()));
  }
}
