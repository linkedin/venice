package com.linkedin.davinci;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;


public class StoreBackendStats extends AbstractVeniceStats {
  private final Sensor badRequestSensor;
  private final Sensor futureVersionSensor;
  private final Sensor currentVersionSensor;
  private final Sensor subscribeDurationSensor;
  private final AtomicReference<Version> currentVersion = new AtomicReference();

  public StoreBackendStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    badRequestSensor = registerSensor("bad_request", new Count());
    futureVersionSensor = registerSensor("future_version_number", new Gauge());
    currentVersionSensor = registerSensor("current_version_number", new Gauge());
    subscribeDurationSensor = registerSensor("subscribe_duration_ms", new Avg(), new Max());

    registerSensor(new AsyncGauge((c, t) -> {
      Version version = currentVersion.get();
      return version != null ? version.getAge().toMillis() : Double.NaN;
    }, "data_age_ms"));
  }

  public void recordBadRequest() {
    badRequestSensor.record();
  }

  public void recordSubscribeDuration(Duration duration) {
    subscribeDurationSensor.record(duration.toMillis());
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
