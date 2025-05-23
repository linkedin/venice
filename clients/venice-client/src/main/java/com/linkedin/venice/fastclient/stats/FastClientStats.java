package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.LONG_TAIL_RETRY;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class FastClientStats extends com.linkedin.venice.client.stats.ClientStats {
  private final String storeName;

  private final Sensor noAvailableReplicaRequestCountSensor;
  private final Sensor dualReadFastClientSlowerRequestCountSensor;
  private final Sensor dualReadFastClientSlowerRequestRatioSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestCountSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestRatioSensor;
  private final Sensor dualReadThinClientFastClientLatencyDeltaSensor;

  private final Sensor leakedRequestCountSensor;
  private final Sensor rejectedRequestCountByLoadControllerSensor;
  private final Sensor rejectionRatioSensor;

  private final Sensor retryRequestWinSensor;
  private final Sensor metadataStalenessSensor;
  private final Sensor fanoutSizeSensor;
  private final Sensor retryFanoutSizeSensor;

  // OTel metrics
  private final MetricEntityStateOneEnum<RequestRetryType> longTailRetry;
  private final MetricEntityStateOneEnum<RequestRetryType> errorRetry;
  private long cacheTimeStampInMs = 0;

  public static FastClientStats getClientStats(
      MetricsRepository metricsRepository,
      String statsPrefix,
      String storeName,
      RequestType requestType) {
    String metricName = statsPrefix.isEmpty() ? storeName : statsPrefix + "." + storeName;
    return new FastClientStats(metricsRepository, metricName, requestType);
  }

  private FastClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType, FAST_CLIENT);

    this.storeName = storeName;
    this.noAvailableReplicaRequestCountSensor =
        registerSensor("no_available_replica_request_count", new OccurrenceRate());

    Rate requestRate = getRequestRate();
    Rate fastClientSlowerRequestRate = new OccurrenceRate();
    this.dualReadFastClientSlowerRequestCountSensor =
        registerSensor("dual_read_fastclient_slower_request_count", fastClientSlowerRequestRate);
    this.dualReadFastClientSlowerRequestRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            fastClientSlowerRequestRate,
            requestRate,
            "dual_read_fastclient_slower_request_ratio"));
    Rate fastClientErrorThinClientSucceedRequestRate = new OccurrenceRate();
    this.dualReadFastClientErrorThinClientSucceedRequestCountSensor = registerSensor(
        "dual_read_fastclient_error_thinclient_succeed_request_count",
        fastClientErrorThinClientSucceedRequestRate);
    this.dualReadFastClientErrorThinClientSucceedRequestRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            fastClientErrorThinClientSucceedRequestRate,
            requestRate,
            "dual_read_fastclient_error_thinclient_succeed_request_ratio"));
    this.dualReadThinClientFastClientLatencyDeltaSensor =
        registerSensorWithDetailedPercentiles("dual_read_thinclient_fastclient_latency_delta", new Max(), new Avg());
    this.leakedRequestCountSensor = registerSensor("leaked_request_count", new OccurrenceRate());
    this.retryRequestWinSensor = registerSensor("retry_request_win", new OccurrenceRate());

    this.metadataStalenessSensor = registerSensor(new AsyncGauge((ignored, ignored2) -> {
      if (this.cacheTimeStampInMs == 0) {
        return Double.NaN;
      } else {
        return System.currentTimeMillis() - this.cacheTimeStampInMs;
      }
    }, "metadata_staleness_high_watermark_ms"));
    this.fanoutSizeSensor = registerSensor("fanout_size", new Avg(), new Max());
    this.retryFanoutSizeSensor = registerSensor("retry_fanout_size", new Avg(), new Max());
    this.rejectedRequestCountByLoadControllerSensor =
        registerSensor("rejected_request_count_by_load_controller", new OccurrenceRate());
    this.rejectionRatioSensor = registerSensor("rejection_ratio", new Avg(), new Max());

    this.longTailRetry = MetricEntityStateOneEnum.create(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.LONG_TAIL_RETRY_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RequestRetryType.class);
    this.errorRetry = MetricEntityStateOneEnum.create(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.ERROR_RETRY_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RequestRetryType.class);
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

  public void recordLongTailRetryRequest() {
    longTailRetry.record(1, LONG_TAIL_RETRY);
  }

  @Override
  public void recordErrorRetryRequest() {
    errorRetry.record(1, ERROR_RETRY);
  }

  public void recordRetryRequestWin() {
    retryRequestWinSensor.record();
  }

  public void updateCacheTimestamp(long cacheTimeStampInMs) {
    this.cacheTimeStampInMs = cacheTimeStampInMs;
  }

  public void recordFanoutSize(int fanoutSize) {
    fanoutSizeSensor.record(fanoutSize);
  }

  public void recordRetryFanoutSize(int retryFanoutSize) {
    retryFanoutSizeSensor.record(retryFanoutSize);
  }

  public void recordRejectedRequestByLoadController() {
    rejectedRequestCountByLoadControllerSensor.record();
  }

  public void recordRejectionRatio(double rejectionRatio) {
    rejectionRatioSensor.record(rejectionRatio);
  }

  /**
   * This method is a utility method to build concise summaries useful in tests
   * and for logging. It generates a single string for all metrics for a sensor
   * @return
   * @param sensorName
   */
  public String buildSensorStatSummary(String sensorName, String... stats) {
    List<Double> metricValues = getMetricValues(sensorName, stats);
    StringBuilder builder = new StringBuilder();
    String sensorFullName = getSensorFullName(sensorName);
    builder.append(sensorFullName).append(":");
    builder.append(
        IntStream.range(0, stats.length)
            .mapToObj((statIdx) -> stats[statIdx] + "=" + metricValues.get(statIdx))
            .collect(Collectors.joining(",")));
    return builder.toString();
  }

  /**
   * This method is a utility method to get metric values useful in tests
   * and for logging.
   * @return
   * @param sensorName
   */
  public List<Double> getMetricValues(String sensorName, String... stats) {
    String sensorFullName = getSensorFullName(sensorName);
    List<Double> collect = Arrays.stream(stats).map((stat) -> {
      Metric metric = getMetricsRepository().getMetric(sensorFullName + "." + stat);
      return (metric != null ? metric.value() : Double.NaN);
    }).collect(Collectors.toList());
    return collect;
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum FastClientTehutiMetricName implements TehutiMetricNameEnum {
    LONG_TAIL_RETRY_REQUEST, ERROR_RETRY_REQUEST;

    private final String metricName;

    FastClientTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
