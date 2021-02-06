package com.linkedin.davinci;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;

import java.util.concurrent.atomic.AtomicReference;


public class StoreBackendStats extends AbstractVeniceStats {
  private final Sensor badRequestSensor;
  private final Sensor futureVersionSensor;
  private final Sensor currentVersionSensor;
  private final AtomicReference<Version> currentVersion = new AtomicReference();

  public StoreBackendStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    badRequestSensor = registerSensor("bad_request", new OccurrenceRate());
    futureVersionSensor = registerSensor("future_version", new Gauge());
    currentVersionSensor = registerSensor("current_version", new Gauge());

    registerSensor("data_age_ms", new Gauge(() -> {
      Version version = currentVersion.get();
      return version != null ? version.getAge().toMillis() : Double.NaN;
    }));
  }

  public void recordBadRequest() {
    badRequestSensor.record();
  }

  public void recordFutureVersion(VersionBackend versionBackend) {
    if (versionBackend != null) {
      futureVersionSensor.record(versionBackend.getVersion().getNumber());
    } else {
      futureVersionSensor.record(Store.NON_EXISTING_VERSION);
    }
  }

  public void recordCurrentVersion(VersionBackend versionBackend) {
    if (versionBackend != null) {
      Version version = versionBackend.getVersion();
      currentVersion.set(version);
      currentVersionSensor.record(version.getNumber());
    } else {
      currentVersion.set(null);
      currentVersionSensor.record(Store.NON_EXISTING_VERSION);
    }
  }
}
