package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_CALL_COUNT;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.METADATA_STALENESS_DURATION;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.REQUEST_FANOUT_COUNT;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.RETRY_REQUEST_WIN_COUNT;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.LONG_TAIL_RETRY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.dimensions.RejectionReason;
import com.linkedin.venice.stats.dimensions.RequestFanoutType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class FastClientStats extends ClientStats {
  private final String storeName;

  private final MetricEntityStateOneEnum<RejectionReason> noAvailableReplicaRequestCount;
  private final MetricEntityStateOneEnum<RejectionReason> rejectedRequestCountByLoadController;
  private final Sensor dualReadFastClientSlowerRequestCountSensor;
  private final Sensor dualReadFastClientSlowerRequestRatioSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestCountSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestRatioSensor;
  private final Sensor dualReadThinClientFastClientLatencyDeltaSensor;

  private final Sensor leakedRequestCountSensor;
  private final MetricEntityStateOneEnum<RejectionReason> rejectionRatio;

  // OTel metrics
  private final MetricEntityStateOneEnum<RequestRetryType> longTailRetry;
  private final MetricEntityStateOneEnum<RequestRetryType> errorRetry;
  private final MetricEntityStateBase retryRequestWin;
  private final AsyncMetricEntityStateBase metadataStalenessHighWatermark;
  private final MetricEntityStateOneEnum<RequestFanoutType> retryFanoutSize;
  private final MetricEntityStateOneEnum<RequestFanoutType> originalFanoutSize;
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
    this.noAvailableReplicaRequestCount = MetricEntityStateOneEnum.create(
        FastClientMetricEntity.REQUEST_REJECTION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.NO_AVAILABLE_REPLICA_REQUEST_COUNT,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RejectionReason.class);

    this.rejectedRequestCountByLoadController = MetricEntityStateOneEnum.create(
        FastClientMetricEntity.REQUEST_REJECTION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.REJECTED_REQUEST_COUNT_BY_LOAD_CONTROLLER,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RejectionReason.class);

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

    this.rejectionRatio = MetricEntityStateOneEnum.create(
        FastClientMetricEntity.REQUEST_REJECTION_RATIO.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.REJECTION_RATIO,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        RejectionReason.class);

    this.longTailRetry = MetricEntityStateOneEnum.create(
        RETRY_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.LONG_TAIL_RETRY_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RequestRetryType.class);
    this.errorRetry = MetricEntityStateOneEnum.create(
        RETRY_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.ERROR_RETRY_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        RequestRetryType.class);

    this.retryRequestWin = MetricEntityStateBase.create(
        RETRY_REQUEST_WIN_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.RETRY_REQUEST_WIN,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        getBaseAttributes());

    // METADATA_STALENESS_DURATION only requires VENICE_STORE_NAME dimension
    Map<VeniceMetricsDimensions, String> metadataStalenessBaseDimensionsMap = null;
    Attributes metadataStalenessBaseAttributes = null;
    if (emitOpenTelemetryMetrics()) {
      metadataStalenessBaseDimensionsMap = Collections.singletonMap(VENICE_STORE_NAME, storeName);
      metadataStalenessBaseAttributes =
          Attributes.builder().put(otelRepository.getDimensionName(VENICE_STORE_NAME), storeName).build();
    }

    this.metadataStalenessHighWatermark = AsyncMetricEntityStateBase.create(
        METADATA_STALENESS_DURATION.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.METADATA_STALENESS_HIGH_WATERMARK_MS,
        Collections.singletonList(
            new AsyncGauge(
                (ignored1, ignored2) -> this.cacheTimeStampInMs == 0
                    ? Double.NaN
                    : System.currentTimeMillis() - this.cacheTimeStampInMs,
                FastClientTehutiMetricName.METADATA_STALENESS_HIGH_WATERMARK_MS.getMetricName())),
        metadataStalenessBaseDimensionsMap,
        metadataStalenessBaseAttributes,
        () -> this.cacheTimeStampInMs == 0 ? 0 : (System.currentTimeMillis() - this.cacheTimeStampInMs));

    // OTel: fanout_size (MIN_MAX_COUNT_SUM_AGGREGATIONS) with dimensions: venice.store.name, venice.request.method,
    // venice.request.fanout_type
    this.retryFanoutSize = MetricEntityStateOneEnum.create(
        REQUEST_FANOUT_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.RETRY_FANOUT_SIZE,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        RequestFanoutType.class);

    this.originalFanoutSize = MetricEntityStateOneEnum.create(
        REQUEST_FANOUT_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        FastClientTehutiMetricName.FANOUT_SIZE,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        RequestFanoutType.class);
  }

  public void recordNoAvailableReplicaRequest() {
    noAvailableReplicaRequestCount.record(1, RejectionReason.NO_REPLICAS_AVAILABLE);
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
    retryRequestWin.record(1);
  }

  public void updateCacheTimestamp(long cacheTimeStampInMs) {
    this.cacheTimeStampInMs = cacheTimeStampInMs;
  }

  public void recordFanoutSize(int size) {
    originalFanoutSize.record(size, RequestFanoutType.ORIGINAL);
  }

  public void recordRetryFanoutSize(int size) {
    retryFanoutSize.record(size, RequestFanoutType.RETRY);
  }

  public void recordRejectedRequestByLoadController() {
    rejectedRequestCountByLoadController.record(1, RejectionReason.THROTTLED_BY_LOAD_CONTROLLER);
  }

  public void recordRejectionRatio(double rejectionRatio) {
    this.rejectionRatio.record(rejectionRatio, RejectionReason.THROTTLED_BY_LOAD_CONTROLLER);
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
    LONG_TAIL_RETRY_REQUEST, ERROR_RETRY_REQUEST, RETRY_REQUEST_WIN, METADATA_STALENESS_HIGH_WATERMARK_MS, FANOUT_SIZE,
    RETRY_FANOUT_SIZE, NO_AVAILABLE_REPLICA_REQUEST_COUNT, REJECTED_REQUEST_COUNT_BY_LOAD_CONTROLLER, REJECTION_RATIO
  }
}
