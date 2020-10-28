package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;


public class ClientStats extends com.linkedin.venice.client.stats.ClientStats {
  private final Sensor noAvailableReplicaRequestCountSensor;

  private final Sensor dualReadFastClientSlowerRequestCountSensor;
  private final Sensor dualReadFastClientSlowerRequestRatioSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestCountSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestRatioSensor;
  private final Sensor dualReadThinClientFastClientLatencyDeltaSensor;



  public static ClientStats getClientStats(MetricsRepository metricsRepository, String statsPrefix, String storeName,
      RequestType requestType) {
    String metricName = statsPrefix.isEmpty() ?  storeName : statsPrefix + "." + storeName;
    return new ClientStats(metricsRepository, metricName, requestType);
  }

  private ClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    this.noAvailableReplicaRequestCountSensor = registerSensor("no_available_replica_request_count", new OccurrenceRate());

    Rate requestRate = getRequestRate();
    Rate fastClientSlowerRequestRate = new OccurrenceRate();
    this.dualReadFastClientSlowerRequestCountSensor = registerSensor("dual_read_fastclient_slower_request_count",
        fastClientSlowerRequestRate);
    this.dualReadFastClientSlowerRequestRatioSensor = registerSensor("dual_read_fastclient_slower_request_ratio",
        new TehutiUtils.SimpleRatioStat(fastClientSlowerRequestRate, requestRate));
    Rate fastClientErrorThinClientSucceedRequestRate = new OccurrenceRate();
    this.dualReadFastClientErrorThinClientSucceedRequestCountSensor = registerSensor(
        "dual_read_fastclient_error_thinclient_succeed_request_count", fastClientErrorThinClientSucceedRequestRate);
    this.dualReadFastClientErrorThinClientSucceedRequestRatioSensor = registerSensor(
        "", new TehutiUtils.SimpleRatioStat(fastClientErrorThinClientSucceedRequestRate, requestRate));
    this.dualReadThinClientFastClientLatencyDeltaSensor = registerSensorWithDetailedPercentiles(
        "dual_read_thinclient_fastclient_latency_delta", new Max(), new Avg());
  }

  public void recordNoAvailableReplicaRequest() {
    noAvailableReplicaRequestCountSensor.record();
  }

  public void recordFastClientSlowerRequest() {
    dualReadFastClientSlowerRequestCountSensor.record();
  }

  public void recordFastClientErrorThinClientSucceedRequest() {
    dualReadFastClientErrorThinClientSucceedRequestCountSensor.record();
  }

  public void recordThinClientFastClientLatencyDelta(double latencyDelta) {
    dualReadThinClientFastClientLatencyDeltaSensor.record(latencyDelta);
  }
}
