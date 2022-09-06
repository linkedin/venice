package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


public class DnsLookupStats extends AbstractVeniceStats {
  private final Sensor dnsLookupRequest;
  private final Sensor dnsLookupLatency;

  public DnsLookupStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    dnsLookupRequest = registerSensor("dns_lookup_request", new Count());
    dnsLookupLatency = registerSensor("dns_lookup_latency", new Avg(), new Max());
  }

  public void recordLookupLatency(long latency) {
    dnsLookupRequest.record();
    dnsLookupLatency.record(latency);
  }
}
