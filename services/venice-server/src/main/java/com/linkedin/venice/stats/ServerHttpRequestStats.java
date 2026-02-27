package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_CALL_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_CALL_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_KEY_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_KEY_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_REQUEST_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_FLUSH_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_KEY_NOT_FOUND_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.READ_RESPONSE_VALUE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_COMPUTE_OPERATION_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_DESERIALIZATION_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_SERIALIZATION_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUERY_TIME;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_SIZE;
import static com.linkedin.davinci.stats.ServerReadOtelMetricEntity.STORAGE_ENGINE_QUEUE_WAIT_TIME;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceComputeOperationType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricAttributesData;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


/**
 * {@code ServerHttpRequestStats} contains counters measuring the performance of handling requests from Routers.
 * Both OTel {@link com.linkedin.venice.stats.metrics.MetricEntityState} fields and Tehuti {@link Sensor} fields
 * coexist in this class. The record methods on MetricEntityState subclasses write to both systems.
 */
public class ServerHttpRequestStats extends AbstractVeniceHttpStats {
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> successRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> errorRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> successRequestLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> errorRequestLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> responseValueSizeMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> responseSizeMetric;
  private final MetricEntityStateOneEnum<VeniceChunkingStatus> storageEngineQueryTimeMetric;
  private final MetricEntityStateOneEnum<VeniceChunkingStatus> readComputeQueryTimeMetric;
  private final MetricEntityStateOneEnum<VeniceComputeOperationType> dotProductCountMetric;
  private final MetricEntityStateOneEnum<VeniceComputeOperationType> cosineCountMetric;
  private final MetricEntityStateOneEnum<VeniceComputeOperationType> hadamardCountMetric;
  private final MetricEntityStateOneEnum<VeniceComputeOperationType> countOperatorCountMetric;
  private final MetricEntityStateBase requestKeyCountMetric;
  private final MetricEntityStateBase requestSizeMetric;
  private final MetricEntityStateBase queueWaitTimeMetric;
  private final MetricEntityStateBase queueSizeMetric;
  private final MetricEntityStateBase deserializationTimeMetric;
  private final MetricEntityStateBase serializationTimeMetric;
  private final MetricEntityStateBase requestKeySizeMetric;
  private final MetricEntityStateBase flushTimeMetric;
  private final MetricEntityStateBase keyNotFoundMetric;
  private final MetricEntityStateBase storageEngineChunkedValueCountMetric;
  private final Sensor databaseLookupLatencyForSmallValueSensor;
  private final Sensor databaseLookupLatencyForLargeValueSensor;
  private final Sensor readComputeLatencyForSmallValueSensor;
  private final Sensor readComputeLatencyForLargeValueSensor;
  private final Sensor readComputeEfficiencySensor;
  // Ratio sensors are not directly written to, but they still get their state updated indirectly
  @SuppressWarnings("unused")
  private final Sensor successRequestRatioSensor;
  private final Sensor earlyTerminatedEarlyRequestCountSensor;
  private final Sensor misroutedStoreVersionSensor;

  private static final MetricsRepository dummySystemStoreMetricRepo = new MetricsRepository();

  public ServerHttpRequestStats(
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ServerHttpRequestStats totalStats,
      boolean isDaVinciClient) {
    super(isDaVinciClient ? dummySystemStoreMetricRepo : metricsRepository, storeName, requestType);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .isTotalStats(isTotalStats())
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .setRequestType(requestType)
            .build();

    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    Map<VeniceMetricsDimensions, String> computeBaseDimensionsMap = null;
    if (baseDimensionsMap != null) {
      computeBaseDimensionsMap = new HashMap<>(baseDimensionsMap);
      computeBaseDimensionsMap.remove(VeniceMetricsDimensions.VENICE_REQUEST_METHOD);
    }

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate successRequest = new OccurrenceRate();
    Rate errorRequest = new OccurrenceRate();

    successRequestMetric = MetricEntityStateThreeEnums.create(
        READ_CALL_COUNT.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.successRequestMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.SUCCESS_REQUEST,
        Arrays.asList(successRequest),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    errorRequestMetric = MetricEntityStateThreeEnums.create(
        READ_CALL_COUNT.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.errorRequestMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.ERROR_REQUEST,
        Arrays.asList(errorRequest),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    successRequestRatioSensor = registerSensor(
        "success_request_ratio",
        new TehutiUtils.RatioStat(successRequest, errorRequest, "success_request_ratio"));

    successRequestLatencyMetric = MetricEntityStateThreeEnums.create(
        READ_CALL_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.successRequestLatencyMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.SUCCESS_REQUEST_LATENCY,
        Arrays.asList(
            TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("success_request_latency"))),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    errorRequestLatencyMetric = MetricEntityStateThreeEnums.create(
        READ_CALL_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.errorRequestLatencyMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.ERROR_REQUEST_LATENCY,
        Arrays
            .asList(TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("error_request_latency"))),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    responseSizeMetric = MetricEntityStateThreeEnums.create(
        READ_RESPONSE_SIZE.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.responseSizeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.RESPONSE_SIZE,
        Arrays.asList(TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_size"))),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    storageEngineQueryTimeMetric = MetricEntityStateOneEnum.create(
        STORAGE_ENGINE_QUERY_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.storageEngineQueryTimeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.STORAGE_ENGINE_QUERY_LATENCY,
        Arrays.asList(
            TehutiUtils.get99PercentileStatWithAvgAndMax(getName(), getFullMetricName("storage_engine_query_latency"))),
        baseDimensionsMap,
        VeniceChunkingStatus.class);

    // Shares STORAGE_ENGINE_QUERY_TIME OTel entity with storageEngineQueryTimeMetric; each has its own Tehuti sensor.
    readComputeQueryTimeMetric = MetricEntityStateOneEnum.create(
        STORAGE_ENGINE_QUERY_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.readComputeQueryTimeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_LATENCY,
        Arrays.asList(
            TehutiUtils
                .getPercentileStatWithAvgAndMax(getName(), getFullMetricName("storage_engine_read_compute_latency"))),
        baseDimensionsMap,
        VeniceChunkingStatus.class);

    databaseLookupLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_small_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForSmallValueSensor,
        TehutiUtils.get99PercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_small_value")));
    databaseLookupLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_large_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForLargeValueSensor,
        TehutiUtils.get99PercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_large_value")));

    List<MeasurableStat> largeValueLookupStats = new ArrayList<>();
    largeValueLookupStats.add(new Max(0));
    largeValueLookupStats.add(new OccurrenceRate());
    if (RequestType.MULTI_GET == requestType) {
      largeValueLookupStats.add(new Avg());
      largeValueLookupStats.add(new Rate());
    }

    storageEngineChunkedValueCountMetric = MetricEntityStateBase.create(
        STORAGE_ENGINE_QUERY_CHUNKED_VALUE_COUNT.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null
                ? new Sensor[] { totalStats.storageEngineChunkedValueCountMetric.getTehutiSensor() }
                : null,
            stats),
        ServerTehutiMetricName.STORAGE_ENGINE_LARGE_VALUE_LOOKUP,
        largeValueLookupStats,
        baseDimensionsMap,
        baseAttributes);

    // Queue metrics: Tehuti total-only; per-store recording routes to total's Tehuti sensor. OTel records per-store.
    if (totalStats == null) {
      queueWaitTimeMetric = MetricEntityStateBase.create(
          STORAGE_ENGINE_QUEUE_WAIT_TIME.getMetricEntity(),
          otelRepository,
          this::registerSensorFinal,
          ServerTehutiMetricName.STORAGE_EXECUTION_HANDLER_SUBMISSION_WAIT_TIME,
          Arrays.asList(
              TehutiUtils.get99PercentileStatWithAvgAndMax(
                  getName(),
                  getFullMetricName("storage_execution_handler_submission_wait_time"))),
          baseDimensionsMap,
          baseAttributes);
    } else {
      queueWaitTimeMetric = MetricEntityStateBase
          .create(STORAGE_ENGINE_QUEUE_WAIT_TIME.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes);
      // Wire per-store recording to total's Tehuti sensor (replicates old registerOnlyTotalSensor behavior)
      queueWaitTimeMetric.setTehutiSensor(totalStats.queueWaitTimeMetric.getTehutiSensor());
    }

    if (totalStats == null) {
      queueSizeMetric = MetricEntityStateBase.create(
          STORAGE_ENGINE_QUEUE_SIZE.getMetricEntity(),
          otelRepository,
          this::registerSensorFinal,
          ServerTehutiMetricName.STORAGE_EXECUTION_QUEUE_LEN,
          Arrays.asList(new Max(), new Avg()),
          baseDimensionsMap,
          baseAttributes);
    } else {
      queueSizeMetric = MetricEntityStateBase
          .create(STORAGE_ENGINE_QUEUE_SIZE.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes);
      // Wire per-store recording to total's Tehuti sensor (replicates old registerOnlyTotalSensor behavior)
      queueSizeMetric.setTehutiSensor(totalStats.queueSizeMetric.getTehutiSensor());
    }

    if (requestType != RequestType.SINGLE_GET) {
      requestKeyCountMetric = MetricEntityStateBase.create(
          READ_REQUEST_KEY_COUNT.getMetricEntity(),
          otelRepository,
          (name, stats) -> registerSensor(
              name,
              totalStats != null ? new Sensor[] { totalStats.requestKeyCountMetric.getTehutiSensor() } : null,
              stats),
          ServerTehutiMetricName.REQUEST_KEY_COUNT,
          Arrays.asList(new Rate(), new OccurrenceRate(), new Avg(), new Max()),
          baseDimensionsMap,
          baseAttributes);
    } else {
      requestKeyCountMetric = MetricEntityStateBase
          .create(READ_REQUEST_KEY_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes);
    }

    keyNotFoundMetric = MetricEntityStateBase.create(
        READ_RESPONSE_KEY_NOT_FOUND_COUNT.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.keyNotFoundMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.KEY_NOT_FOUND,
        Arrays.asList(new Rate()),
        baseDimensionsMap,
        baseAttributes);

    requestSizeMetric = MetricEntityStateBase.create(
        READ_REQUEST_SIZE.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.requestSizeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.REQUEST_SIZE_IN_BYTES,
        Arrays.asList(new Avg(), new Min(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    readComputeLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_small_value",
        totalStats,
        () -> totalStats.readComputeLatencyForSmallValueSensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_latency_for_small_value")));
    readComputeLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_large_value",
        totalStats,
        () -> totalStats.readComputeLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_latency_for_large_value")));

    readComputeEfficiencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_efficiency",
        totalStats,
        () -> totalStats.readComputeEfficiencySensor,
        new Avg(),
        new Min(),
        new Max());

    deserializationTimeMetric = MetricEntityStateBase.create(
        STORAGE_ENGINE_QUERY_DESERIALIZATION_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.deserializationTimeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_DESERIALIZATION_LATENCY,
        Arrays.asList(
            TehutiUtils.getPercentileStatWithAvgAndMax(
                getName(),
                getFullMetricName("storage_engine_read_compute_deserialization_latency"))),
        baseDimensionsMap,
        baseAttributes);

    serializationTimeMetric = MetricEntityStateBase.create(
        STORAGE_ENGINE_QUERY_SERIALIZATION_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.serializationTimeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.STORAGE_ENGINE_READ_COMPUTE_SERIALIZATION_LATENCY,
        Arrays.asList(
            TehutiUtils.getPercentileStatWithAvgAndMax(
                getName(),
                getFullMetricName("storage_engine_read_compute_serialization_latency"))),
        baseDimensionsMap,
        baseAttributes);

    if (requestType == RequestType.COMPUTE) {
      dotProductCountMetric = createComputeOpMetric(
          otelRepository,
          totalStats != null ? totalStats.dotProductCountMetric : null,
          ServerTehutiMetricName.DOT_PRODUCT_COUNT,
          computeBaseDimensionsMap);
      cosineCountMetric = createComputeOpMetric(
          otelRepository,
          totalStats != null ? totalStats.cosineCountMetric : null,
          ServerTehutiMetricName.COSINE_SIMILARITY_COUNT,
          computeBaseDimensionsMap);
      hadamardCountMetric = createComputeOpMetric(
          otelRepository,
          totalStats != null ? totalStats.hadamardCountMetric : null,
          ServerTehutiMetricName.HADAMARD_PRODUCT_COUNT,
          computeBaseDimensionsMap);
      countOperatorCountMetric = createComputeOpMetric(
          otelRepository,
          totalStats != null ? totalStats.countOperatorCountMetric : null,
          ServerTehutiMetricName.COUNT_OPERATOR_COUNT,
          computeBaseDimensionsMap);
    } else {
      dotProductCountMetric = null;
      cosineCountMetric = null;
      hadamardCountMetric = null;
      countOperatorCountMetric = null;
    }

    earlyTerminatedEarlyRequestCountSensor = registerPerStoreAndTotal(
        "early_terminated_request_count",
        totalStats,
        () -> totalStats.earlyTerminatedEarlyRequestCountSensor,
        new OccurrenceRate());

    if (isKeyValueProfilingEnabled || requestType == RequestType.SINGLE_GET) {
      final MeasurableStat[] valueSizeStats;
      final MeasurableStat[] keySizeStats;
      if (isKeyValueProfilingEnabled) {
        valueSizeStats =
            TehutiUtils.getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName("request_value_size"));
        keySizeStats =
            TehutiUtils.getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName("request_key_size"));
      } else {
        valueSizeStats = new MeasurableStat[] { new Avg(), new Max() };
        keySizeStats = new MeasurableStat[] { new Avg(), new Max() };
      }

      responseValueSizeMetric = MetricEntityStateThreeEnums.create(
          READ_RESPONSE_VALUE_SIZE.getMetricEntity(),
          otelRepository,
          (name, stats) -> registerSensor(
              name,
              totalStats != null ? new Sensor[] { totalStats.responseValueSizeMetric.getTehutiSensor() } : null,
              stats),
          ServerTehutiMetricName.REQUEST_VALUE_SIZE,
          Arrays.asList(valueSizeStats),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      requestKeySizeMetric = MetricEntityStateBase.create(
          READ_REQUEST_KEY_SIZE.getMetricEntity(),
          otelRepository,
          (name, stats) -> registerSensor(
              name,
              totalStats != null ? new Sensor[] { totalStats.requestKeySizeMetric.getTehutiSensor() } : null,
              stats),
          ServerTehutiMetricName.REQUEST_KEY_SIZE,
          Arrays.asList(keySizeStats),
          baseDimensionsMap,
          baseAttributes);
    } else {
      responseValueSizeMetric = MetricEntityStateThreeEnums.create(
          READ_RESPONSE_VALUE_SIZE.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      requestKeySizeMetric = MetricEntityStateBase
          .create(READ_REQUEST_KEY_SIZE.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes);
    }

    misroutedStoreVersionSensor = registerPerStoreAndTotal(
        "misrouted_store_version_request_count",
        totalStats,
        () -> totalStats.misroutedStoreVersionSensor,
        new OccurrenceRate());

    flushTimeMetric = MetricEntityStateBase.create(
        READ_RESPONSE_FLUSH_TIME.getMetricEntity(),
        otelRepository,
        (name, stats) -> registerSensor(
            name,
            totalStats != null ? new Sensor[] { totalStats.flushTimeMetric.getTehutiSensor() } : null,
            stats),
        ServerTehutiMetricName.FLUSH_LATENCY,
        Arrays.asList(TehutiUtils.getPercentileStat(getName(), getFullMetricName("flush_latency"))),
        baseDimensionsMap,
        baseAttributes);
  }

  private Sensor registerPerStoreAndTotal(
      String sensorName,
      ServerHttpRequestStats totalStats,
      Supplier<Sensor> totalSensor,
      MeasurableStat... stats) {
    Sensor[] parent = totalStats == null ? null : new Sensor[] { totalSensor.get() };
    return registerSensor(sensorName, parent, stats);
  }

  /** Only private/static/final methods can be passed as TehutiSensorRegistrationFunction. */
  private Sensor registerSensorFinal(String sensorName, MeasurableStat... stats) {
    return this.registerSensor(sensorName, stats);
  }

  private MetricEntityStateOneEnum<VeniceComputeOperationType> createComputeOpMetric(
      VeniceOpenTelemetryMetricsRepository otelRepo,
      MetricEntityStateOneEnum<VeniceComputeOperationType> totalMetric,
      ServerTehutiMetricName tehutiName,
      Map<VeniceMetricsDimensions, String> baseDims) {
    return MetricEntityStateOneEnum.create(
        STORAGE_ENGINE_COMPUTE_OPERATION_COUNT.getMetricEntity(),
        otelRepo,
        (name, stats) -> registerSensor(
            name,
            totalMetric != null ? new Sensor[] { totalMetric.getTehutiSensor() } : null,
            stats),
        tehutiName,
        Arrays.asList(new Avg(), new Total()),
        baseDims,
        VeniceComputeOperationType.class);
  }

  public void recordSuccessRequest(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    successRequestMetric.record(1, statusEnum, statusCategory, veniceCategory);
  }

  public void recordErrorRequest(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    errorRequestMetric.record(1, statusEnum, statusCategory, veniceCategory);
  }

  public void recordSuccessRequestLatency(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory,
      double latency) {
    successRequestLatencyMetric.record(latency, statusEnum, statusCategory, veniceCategory);
  }

  public void recordErrorRequestLatency(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory,
      double latency) {
    errorRequestLatencyMetric.record(latency, statusEnum, statusCategory, veniceCategory);
  }

  public void recordDatabaseLookupLatency(double latency, boolean assembledMultiChunkLargeValue) {
    VeniceChunkingStatus chunkingStatus =
        assembledMultiChunkLargeValue ? VeniceChunkingStatus.CHUNKED : VeniceChunkingStatus.UNCHUNKED;
    storageEngineQueryTimeMetric.record(latency, chunkingStatus);
    if (assembledMultiChunkLargeValue) {
      databaseLookupLatencyForLargeValueSensor.record(latency);
    } else {
      databaseLookupLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountMetric.record(keyCount);
  }

  public void recordKeyNotFoundCount(int count) {
    keyNotFoundMetric.record(count);
  }

  public void recordRequestSizeInBytes(int requestSizeInBytes) {
    requestSizeMetric.record(requestSizeInBytes);
  }

  public void recordMultiChunkLargeValueCount(int multiChunkLargeValueCount) {
    storageEngineChunkedValueCountMetric.record(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    queueWaitTimeMetric.record(submissionWaitTime);
  }

  public void recordStorageExecutionQueueLen(int len) {
    queueSizeMetric.record(len);
  }

  public void recordReadComputeLatency(double latency, boolean assembledMultiChunkLargeValue) {
    VeniceChunkingStatus chunkingStatus =
        assembledMultiChunkLargeValue ? VeniceChunkingStatus.CHUNKED : VeniceChunkingStatus.UNCHUNKED;
    readComputeQueryTimeMetric.record(latency, chunkingStatus);
    if (assembledMultiChunkLargeValue) {
      readComputeLatencyForLargeValueSensor.record(latency);
    } else {
      readComputeLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordReadComputeDeserializationLatency(double latency, boolean assembledMultiChunkLargeValue) {
    deserializationTimeMetric.record(latency);
  }

  public void recordReadComputeSerializationLatency(double latency, boolean assembledMultiChunkLargeValue) {
    serializationTimeMetric.record(latency);
  }

  public void recordReadComputeEfficiency(double efficiency) {
    readComputeEfficiencySensor.record(efficiency);
  }

  public void recordDotProductCount(int count) {
    if (dotProductCountMetric != null) {
      dotProductCountMetric.record(count, VeniceComputeOperationType.DOT_PRODUCT);
    }
  }

  public void recordCosineSimilarityCount(int count) {
    if (cosineCountMetric != null) {
      cosineCountMetric.record(count, VeniceComputeOperationType.COSINE_SIMILARITY);
    }
  }

  public void recordHadamardProductCount(int count) {
    if (hadamardCountMetric != null) {
      hadamardCountMetric.record(count, VeniceComputeOperationType.HADAMARD_PRODUCT);
    }
  }

  public void recordCountOperatorCount(int count) {
    if (countOperatorCountMetric != null) {
      countOperatorCountMetric.record(count, VeniceComputeOperationType.COUNT);
    }
  }

  public void recordEarlyTerminatedEarlyRequest() {
    earlyTerminatedEarlyRequestCountSensor.record();
  }

  public void recordKeySizeInByte(int keySize) {
    requestKeySizeMetric.record(keySize);
  }

  /** OTel-only value size with HTTP status dims; Tehuti is recorded per-key via {@link #recordValueSizeInByte}. */
  public void recordValueSizeInByteOtelOnly(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory,
      int valueSize) {
    Attributes attrs = responseValueSizeMetric.getAttributes(statusEnum, statusCategory, veniceCategory);
    if (attrs != null) {
      responseValueSizeMetric.recordOtelMetric(valueSize, new MetricAttributesData(attrs));
    }
  }

  /** Tehuti-only value size; OTel aggregate is via {@link #recordValueSizeInByteOtelOnly}. */
  public void recordValueSizeInByte(int valueSize) {
    Sensor tehutiSensor = responseValueSizeMetric.getTehutiSensor();
    if (tehutiSensor != null) {
      tehutiSensor.record(valueSize);
    }
  }

  public void recordMisroutedStoreVersionRequest() {
    misroutedStoreVersionSensor.record();
  }

  public void recordFlushLatency(double latency) {
    flushTimeMetric.record(latency);
  }

  /** OTel-only response size with HTTP status dims; Tehuti is via {@link #recordResponseSize(int)}. */
  public void recordResponseSizeOtelOnly(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory,
      int size) {
    Attributes attrs = responseSizeMetric.getAttributes(statusEnum, statusCategory, veniceCategory);
    if (attrs != null) {
      responseSizeMetric.recordOtelMetric(size, new MetricAttributesData(attrs));
    }
  }

  /** Tehuti-only response size; OTel is via {@link #recordResponseSizeOtelOnly}. */
  public void recordResponseSize(int size) {
    Sensor tehutiSensor = responseSizeMetric.getTehutiSensor();
    if (tehutiSensor != null) {
      tehutiSensor.record(size);
    }
  }

  enum ServerTehutiMetricName implements TehutiMetricNameEnum {
    SUCCESS_REQUEST, ERROR_REQUEST, SUCCESS_REQUEST_LATENCY, ERROR_REQUEST_LATENCY, STORAGE_ENGINE_QUERY_LATENCY,
    STORAGE_ENGINE_READ_COMPUTE_LATENCY, STORAGE_ENGINE_LARGE_VALUE_LOOKUP, REQUEST_KEY_COUNT, REQUEST_SIZE_IN_BYTES,
    STORAGE_EXECUTION_HANDLER_SUBMISSION_WAIT_TIME, STORAGE_EXECUTION_QUEUE_LEN,
    STORAGE_ENGINE_READ_COMPUTE_DESERIALIZATION_LATENCY, STORAGE_ENGINE_READ_COMPUTE_SERIALIZATION_LATENCY,
    DOT_PRODUCT_COUNT, COSINE_SIMILARITY_COUNT, HADAMARD_PRODUCT_COUNT, COUNT_OPERATOR_COUNT, KEY_NOT_FOUND,
    REQUEST_KEY_SIZE, REQUEST_VALUE_SIZE, FLUSH_LATENCY, RESPONSE_SIZE
  }
}
