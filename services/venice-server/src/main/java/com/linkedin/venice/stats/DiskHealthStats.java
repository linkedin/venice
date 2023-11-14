package com.linkedin.venice.stats;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;


/**
 * {@code DiskHealthStats} measures the disk health conditions based on the periodic tests ran by the {@link DiskHealthCheckService}.
 */
public class DiskHealthStats extends AbstractVeniceStats {
  private DiskHealthCheckService diskHealthCheckService;

  private Sensor diskHealthSensor;

  public DiskHealthStats(
      MetricsRepository metricsRepository,
      DiskHealthCheckService diskHealthCheckService,
      String name) {
    super(metricsRepository, name);
    this.diskHealthCheckService = diskHealthCheckService;

    diskHealthSensor = registerSensor(new AsyncGauge((c, t) -> {
      if (this.diskHealthCheckService.isDiskHealthy()) {
        // report 1 if the disk in this host is healthy; otherwise, report 0.
        return 1;
      } else {
        return 0;
      }
    }, "disk_healthy"));
  }
}
