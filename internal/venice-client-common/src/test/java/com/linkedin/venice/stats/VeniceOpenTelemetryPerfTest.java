package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformHttpResponseStatusToHttpResponseStatusEnum;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Rate;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class VeniceOpenTelemetryPerfTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryPerfTest.class);

  // Common dimension arrays used across tests
  private static final HttpResponseStatusEnum[] STATUS_ENUMS =
      { HttpResponseStatusEnum.OK, HttpResponseStatusEnum.BAD_REQUEST, HttpResponseStatusEnum.INTERNAL_SERVER_ERROR,
          HttpResponseStatusEnum.NOT_FOUND };
  private static final HttpResponseStatusCodeCategory[] CATEGORY_ENUMS = { HttpResponseStatusCodeCategory.SUCCESS,
      HttpResponseStatusCodeCategory.CLIENT_ERROR, HttpResponseStatusCodeCategory.SERVER_ERROR };
  private static final VeniceResponseStatusCategory[] RESPONSE_CATEGORIES = VeniceResponseStatusCategory.values();

  // Common 6-dimension set for three-enum metrics
  private static final Set<VeniceMetricsDimensions> THREE_ENUM_DIMENSIONS = Utils.setOf(
      VeniceMetricsDimensions.VENICE_STORE_NAME,
      VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
      VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
      VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
      VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
      VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY);

  private static String formatNumber(long number) {
    return NumberFormat.getNumberInstance(Locale.US).format(number);
  }

  private static Map<VeniceMetricsDimensions, String> createBaseDimensions() {
    Map<VeniceMetricsDimensions, String> dimensions = new HashMap<>();
    dimensions.put(VeniceMetricsDimensions.VENICE_STORE_NAME, "test_store");
    dimensions.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, "test_cluster");
    return dimensions;
  }

  private static Map<VeniceMetricsDimensions, String> createBaseDimensionsWithRequestMethod() {
    Map<VeniceMetricsDimensions, String> dimensions = createBaseDimensions();
    dimensions.put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, "multi_get_streaming");
    return dimensions;
  }

  private static MetricEntity createThreeEnumMetricEntity(String name, MetricType metricType, String description) {
    return new MetricEntity(name, metricType, MetricUnit.NUMBER, description, THREE_ENUM_DIMENSIONS);
  }

  private static void printBenchmarkResults(
      String approach1Name,
      long approach1DurationNs,
      String approach2Name,
      long approach2DurationNs,
      int totalOperations) {
    LOGGER.info("========== Results ==========");
    LOGGER.info(approach1Name + ":");
    LOGGER.info(
        String.format(
            "  Total: %s ms | Avg: %.2f ns/op | Throughput: %s ops/s",
            formatNumber(approach1DurationNs / 1_000_000),
            (double) approach1DurationNs / totalOperations,
            formatNumber((long) totalOperations * 1_000_000_000L / approach1DurationNs)));

    LOGGER.info(approach2Name + ":");
    LOGGER.info(
        String.format(
            "  Total: %s ms | Avg: %.2f ns/op | Throughput: %s ops/s",
            formatNumber(approach2DurationNs / 1_000_000),
            (double) approach2DurationNs / totalOperations,
            formatNumber((long) totalOperations * 1_000_000_000L / approach2DurationNs)));

    double ratio = (double) approach1DurationNs / approach2DurationNs;
    double speedup = ratio > 1 ? ratio : 1.0 / ratio;
    LOGGER.info(
        String.format(
            "Comparison: %s is %.2fx %s than %s",
            approach1Name,
            speedup,
            ratio > 1 ? "slower" : "faster",
            approach2Name));
  }

  private VeniceOpenTelemetryMetricsRepository createOtelRepository() {
    io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader inMemoryReader =
        io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader.create();

    VeniceMetricsConfig config = new VeniceMetricsConfig.Builder().setServiceName("venice-perf-test")
        .setMetricPrefix("test")
        .setEmitOtelMetrics(true)
        .emitTehutiMetrics(false)
        .setUseOtelExponentialHistogram(false)
        .build();
    config.setOtelAdditionalMetricsReader(inMemoryReader);
    return new VeniceOpenTelemetryMetricsRepository(config);
  }

  // Marking this as flaky as we don't want to run this test in every build.
  @Test(groups = "flaky")
  public void testGeneratingAttributes() {
    // config
    boolean createAttributes = true;
    int numStores = 500;
    int iterations = 1000000000;

    List<MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory>> metricList =
        new ArrayList<>();
    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseMetricDimensionsMap = new HashMap<>();
    baseMetricDimensionsMap.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, "test_cluster");
    baseMetricDimensionsMap.put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, "multi_get_streaming");
    MetricEntity metricEntity = new MetricEntity(
        "test_metric",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "testDescription",
        Utils.setOf(
            VeniceMetricsDimensions.VENICE_STORE_NAME,
            VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
            VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY));
    HttpResponseStatus[] possibleStatuses = { HttpResponseStatus.OK, HttpResponseStatus.BAD_REQUEST,
        HttpResponseStatus.INTERNAL_SERVER_ERROR, HttpResponseStatus.NOT_FOUND, HttpResponseStatus.NO_CONTENT,
        HttpResponseStatus.CREATED, HttpResponseStatus.ACCEPTED, HttpResponseStatus.MOVED_PERMANENTLY,
        HttpResponseStatus.FOUND, HttpResponseStatus.SEE_OTHER, HttpResponseStatus.NOT_MODIFIED,
        HttpResponseStatus.USE_PROXY, HttpResponseStatus.TEMPORARY_REDIRECT, HttpResponseStatus.PERMANENT_REDIRECT,
        HttpResponseStatus.BAD_GATEWAY, HttpResponseStatus.GATEWAY_TIMEOUT, HttpResponseStatus.SERVICE_UNAVAILABLE,
        HttpResponseStatus.REQUEST_TIMEOUT, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
        HttpResponseStatus.REQUEST_URI_TOO_LONG, HttpResponseStatus.EXPECTATION_FAILED,
        HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
        HttpResponseStatus.PRECONDITION_FAILED, HttpResponseStatus.TOO_MANY_REQUESTS };
    VeniceResponseStatusCategory[] responseCategories = VeniceResponseStatusCategory.values();

    long startTimeInit = System.currentTimeMillis();
    for (int i = 0; i < numStores; i++) {
      baseMetricDimensionsMap.put(VeniceMetricsDimensions.VENICE_STORE_NAME, "test_store_medium_sized_name" + i);
      metricList.add(
          MetricEntityStateThreeEnums.create(
              metricEntity,
              otelRepository,
              baseMetricDimensionsMap,
              HttpResponseStatusEnum.class,
              HttpResponseStatusCodeCategory.class,
              VeniceResponseStatusCategory.class));
    }
    long endTimeInit = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      int j = RandomGenUtils.getRandomIntWithin(metricList.size());
      MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricEntityState =
          metricList.get(j);
      j = RandomGenUtils.getRandomIntWithin(possibleStatuses.length);
      HttpResponseStatus httpResponseStatus = possibleStatuses[j];
      HttpResponseStatusEnum httpResponseStatusEnum =
          transformHttpResponseStatusToHttpResponseStatusEnum(httpResponseStatus);
      HttpResponseStatusCodeCategory httpResponseStatusCodeCategory =
          getVeniceHttpResponseStatusCodeCategory(httpResponseStatus);
      j = RandomGenUtils.getRandomIntWithin(responseCategories.length);
      VeniceResponseStatusCategory veniceResponseStatusCategory = responseCategories[j];
      if (createAttributes) {
        Attributes attributes = metricEntityState
            .getAttributes(httpResponseStatusEnum, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
        assertEquals(attributes.size(), 6);
      }
    }
    long endTimeGettingAttributesDuringRuntime = System.currentTimeMillis();
    LOGGER.info("Attribution creation enabled: " + createAttributes);
    LOGGER.info("Number of loops: " + formatNumber(iterations));
    LOGGER.info("Number of stores: " + numStores);
    LOGGER.info("Total time taken: " + (endTimeGettingAttributesDuringRuntime - startTimeInit) + " ms");
    LOGGER.info("Time taken to init: " + (endTimeInit - startTimeInit) + " ms");
    LOGGER.info("Time taken to run test: " + (endTimeGettingAttributesDuringRuntime - endTimeInit) + " ms");
    LOGGER.info(
        "Average time per loop: "
            + formatNumber((int) ((endTimeGettingAttributesDuringRuntime - endTimeInit) / iterations)) + " ms");
  }

  /**
   * Compares performance of recording metrics using Tehuti vs OpenTelemetry.
   * Otel should be faster than tehuti.
   */
  @Test
  public void testTehutiVsOtelMetricRecordingPerf() {
    int numLoopsForWarmUp = 1000; // 1k
    // 100k iterations x 1k internal loops = 100M total recordings per approach
    int numLoops = 100000;
    int numInternalLoops = 1000;

    // Setup test fixtures
    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensions = createBaseDimensionsWithRequestMethod();
    MetricEntity callCountMetric = createThreeEnumMetricEntity("call_count", MetricType.HISTOGRAM, "Histogram");

    // Create OTel-only metric
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricOtelOnly =
        MetricEntityStateThreeEnums.create(
            callCountMetric,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Create Tehuti-only metric
    MetricsRepository tehutiRepository = new MetricsRepository();
    List<MeasurableStat> tehutiStats = new ArrayList<>();
    tehutiStats.add(new Avg());
    tehutiStats.add(new Rate());
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricTehutiOnly =
        MetricEntityStateThreeEnums.create(callCountMetric, null, (name, stats) -> {
          Sensor sensor = tehutiRepository.sensor(name);
          for (MeasurableStat stat: stats) {
            sensor.add(name + "." + stat.getClass().getSimpleName(), stat);
          }
          return sensor;
        }, new TehutiMetricNameEnum() {
          @Override
          public String getMetricName() {
            return "call_count_tehuti_only";
          }
        },
            tehutiStats,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    LOGGER.info("Starting benchmark with " + formatNumber(numLoops) + " loops...");

    // Warmup
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];
      metricOtelOnly.record(1L, status, category, veniceCategory);
      metricTehutiOnly.record(1L, status, category, veniceCategory);
    }

    long totalOtelTimeNs = 0;
    long totalTehutiTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];

      // Measure Tehuti call
      long startTehuti = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        metricTehutiOnly.record(1L, status, category, veniceCategory);
      }
      long endTehuti = System.nanoTime();
      totalTehutiTimeNs += (endTehuti - startTehuti);

      // Measure OpenTelemetry call
      long startOtel = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        metricOtelOnly.record(1L, status, category, veniceCategory);
      }
      long endOtel = System.nanoTime();
      totalOtelTimeNs += (endOtel - startOtel);
    }

    printBenchmarkResults("OpenTelemetry", totalOtelTimeNs, "Tehuti", totalTehutiTimeNs, numLoops);
    // Allow 10% tolerance for system variability
    assertTrue(
        totalOtelTimeNs < totalTehutiTimeNs * 1.1,
        "OpenTelemetry should not be significantly slower than Tehuti");
  }

  /**
   * Compares performance of AtomicLong increment/decrement operations vs OpenTelemetry UpDownCounter
   * to measure which approach is faster for tracking gauge-like metrics.
   *
   * AtomicLong is expected to be significantly faster than OpenTelemetry UpDownCounter.
   */
  @Test
  public void testAtomicLongVsOtelUpDownCounterPerf() {
    int numLoopsForWarmUp = 1000; // 1k
    // 100k iterations x 1k internal loops = 100M total recordings per approach
    int numLoops = 100000;
    int numInternalLoops = 1000;

    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = createBaseDimensions();

    MetricEntity upDownCounterMetric = new MetricEntity(
        "active_connections",
        MetricType.UP_DOWN_COUNTER,
        MetricUnit.NUMBER,
        "Tracks active connections",
        Utils.setOf(VeniceMetricsDimensions.VENICE_STORE_NAME, VeniceMetricsDimensions.VENICE_CLUSTER_NAME));

    Attributes baseAttributes = otelRepository.createAttributes(upDownCounterMetric, baseDimensionsMap);
    MetricEntityStateBase otelUpDownCounter =
        MetricEntityStateBase.create(upDownCounterMetric, otelRepository, baseDimensionsMap, baseAttributes);
    AtomicLong atomicCounter = new AtomicLong(0);

    LOGGER.info("Starting benchmark with " + formatNumber(numLoops) + " loops...");

    // Warmup
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      otelUpDownCounter.record(2L);
      atomicCounter.incrementAndGet();
      otelUpDownCounter.record(-1L);
      atomicCounter.decrementAndGet();
    }

    long totalAtomicTimeNs = 0;
    long totalOtelTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      // Measure AtomicLong operations
      long startAtomic = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        atomicCounter.incrementAndGet();
        atomicCounter.decrementAndGet();
      }
      long endAtomic = System.nanoTime();
      totalAtomicTimeNs += (endAtomic - startAtomic);

      // Measure OpenTelemetry UpDownCounter operations
      long startOtel = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        otelUpDownCounter.record(2L);
        otelUpDownCounter.record(-1L);
      }
      long endOtel = System.nanoTime();
      totalOtelTimeNs += (endOtel - startOtel);
    }

    int totalOperations = numLoops * 2; // increment + decrement per loop
    printBenchmarkResults(
        "AtomicLong",
        totalAtomicTimeNs,
        "OpenTelemetry UpDownCounter",
        totalOtelTimeNs,
        totalOperations);
    assertTrue(totalAtomicTimeNs < totalOtelTimeNs, "AtomicLong should be faster than OpenTelemetry UpDownCounter");
  }

  /**
   * Compare performance of metric recording with ASYNC_COUNTER_FOR_HIGH_PERF_CASES (LongAdder)
   * vs regular COUNTER.
   *
   * ASYNC_COUNTER_FOR_HIGH_PERF_CASES uses LongAdder internally for fast, contention-free recording.
   * Regular COUNTER calls OTel's counter.add()
   */
  @Test
  public void testThreeEnumMetricWithAndWithoutLongAdder() {
    int numLoopsForWarmUp = 1000;
    int numLoops = 100000;
    int numInternalLoops = 1000;

    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensions = createBaseDimensionsWithRequestMethod();

    // Create ASYNC_COUNTER_FOR_HIGH_PERF_CASES metric (uses LongAdder)
    MetricEntity asyncCounterEntity = createThreeEnumMetricEntity(
        "test_async_counter",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        "Test async counter");
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> asyncCounterMetric =
        MetricEntityStateThreeEnums.create(
            asyncCounterEntity,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Create regular COUNTER metric (direct OTel recording)
    MetricEntity counterEntity = createThreeEnumMetricEntity("test_counter", MetricType.COUNTER, "Test counter");
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> counterMetric =
        MetricEntityStateThreeEnums.create(
            counterEntity,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    LOGGER.info("Starting benchmark with " + formatNumber(numLoops) + " loops...");

    // Warmup
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];
      asyncCounterMetric.record(1L, status, category, veniceCategory);
      counterMetric.record(1L, status, category, veniceCategory);
    }

    long totalAsyncCounterTimeNs = 0;
    long totalCounterTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];

      // Measure regular COUNTER (without LongAdder)
      long startCounter = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        counterMetric.record(1L, status, category, veniceCategory);
      }
      long endCounter = System.nanoTime();
      totalCounterTimeNs += (endCounter - startCounter);

      // Measure ASYNC_COUNTER_FOR_HIGH_PERF_CASES (with LongAdder)
      long startAsync = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        asyncCounterMetric.record(1L, status, category, veniceCategory);
      }
      long endAsync = System.nanoTime();
      totalAsyncCounterTimeNs += (endAsync - startAsync);
    }

    int totalOperations = numLoops * numInternalLoops;
    printBenchmarkResults(
        "ASYNC_COUNTER_FOR_HIGH_PERF_CASES (LongAdder)",
        totalAsyncCounterTimeNs,
        "Regular COUNTER",
        totalCounterTimeNs,
        totalOperations);
    assertTrue(totalAsyncCounterTimeNs < totalCounterTimeNs, "Async counter should be faster than regular counter");
  }

  /**
   * Compare performance of ASYNC_COUNTER_FOR_HIGH_PERF_CASES (LongAdder)
   * vs LongAdderRateGauge + Tehuti.
   *
   * Both use LongAdder internally, but have different overhead:
   * - ASYNC_COUNTER_FOR_HIGH_PERF_CASES: EnumMap lookup + LongAdder.add()
   * - LongAdderRateGauge + Tehuti: direct LongAdder.add() + Tehuti sensor.record()
   */
  @Test
  public void testAsyncCounterVsLongAdderRateGaugeWithTehuti() {
    int numLoopsForWarmUp = 1000;
    int numLoops = 100000;
    int numInternalLoops = 1000;

    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensions = createBaseDimensionsWithRequestMethod();

    // Create ASYNC_COUNTER_FOR_HIGH_PERF_CASES metric
    MetricEntity asyncCounterEntity = createThreeEnumMetricEntity(
        "test_async_counter_perf",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        "Test async counter perf");
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> asyncCounterMetric =
        MetricEntityStateThreeEnums.create(
            asyncCounterEntity,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Create LongAdderRateGauge with Tehuti
    MetricsRepository tehutiRepository = new MetricsRepository(new MetricConfig());
    LongAdderRateGauge longAdderRateGauge = new LongAdderRateGauge();
    Sensor tehutiSensor = tehutiRepository.sensor("test_tehuti_sensor");
    tehutiSensor.add("test_tehuti_metric", longAdderRateGauge);

    LOGGER.info("Starting benchmark with " + formatNumber(numLoops) + " loops...");

    // Warmup
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];
      asyncCounterMetric.record(1L, status, category, veniceCategory);
      longAdderRateGauge.record(1L);
      tehutiSensor.record(1.0);
    }

    long totalAsyncCounterTimeNs = 0;
    long totalLongAdderTimeNs = 0;
    long totalTehutiSensorTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
      HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
      VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];

      // Measure ASYNC_COUNTER_FOR_HIGH_PERF_CASES (LongAdder)
      long startAsync = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        asyncCounterMetric.record(1L, status, category, veniceCategory);
      }
      long endAsync = System.nanoTime();
      totalAsyncCounterTimeNs += (endAsync - startAsync);

      // Measure LongAdderRateGauge (no dimensions, just raw LongAdder)
      long startLongAdder = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        longAdderRateGauge.record(1L);
      }
      long endLongAdder = System.nanoTime();
      totalLongAdderTimeNs += (endLongAdder - startLongAdder);

      // Measure Tehuti sensor.record()
      long startTehutiSensor = System.nanoTime();
      for (int j = 0; j < numInternalLoops; j++) {
        tehutiSensor.record(1.0);
      }
      long endTehutiSensor = System.nanoTime();
      totalTehutiSensorTimeNs += (endTehutiSensor - startTehutiSensor);
    }

    // Report results
    int totalOperations = numLoops * numInternalLoops;
    LOGGER.info("=== Performance Comparison: ASYNC_COUNTER_FOR_HIGH_PERF_CASES vs LongAdderRateGauge + Tehuti ===");
    printBenchmarkResults(
        "ASYNC_COUNTER_FOR_HIGH_PERF_CASES",
        totalAsyncCounterTimeNs,
        "LongAdderRateGauge",
        totalLongAdderTimeNs,
        totalOperations);
    printBenchmarkResults(
        "ASYNC_COUNTER_FOR_HIGH_PERF_CASES",
        totalAsyncCounterTimeNs,
        "Tehuti sensor.record()",
        totalTehutiSensorTimeNs,
        totalOperations);
    assertTrue(
        totalLongAdderTimeNs < totalAsyncCounterTimeNs,
        "Async counter should be slower than LongAdderRateGauge");
    assertTrue(
        totalAsyncCounterTimeNs < totalTehutiSensorTimeNs,
        "Async counter should be faster than Tehuti sensor.record()");
    tehutiRepository.close();
  }

  /**
   * Multi-threaded performance comparison.
   * Compare ASYNC_COUNTER_FOR_HIGH_PERF_CASES vs regular COUNTER under concurrent load.
   */
  @Test
  public void testConcurrentAsyncCounterVsCounterPerf() throws InterruptedException {
    int numThreads = 8;
    int numLoopsForWarmUp = 1000;
    int numLoops = 1000000;
    int numLoopsPerThread = numLoops / numThreads;
    int numInternalLoops = 1000;

    VeniceOpenTelemetryMetricsRepository otelRepository = createOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensions = createBaseDimensionsWithRequestMethod();

    // Create ASYNC_COUNTER_FOR_HIGH_PERF_CASES metric
    MetricEntity asyncCounterEntity = createThreeEnumMetricEntity(
        "test_async_counter_concurrent",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        "Test async counter concurrent");
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> asyncCounterMetric =
        MetricEntityStateThreeEnums.create(
            asyncCounterEntity,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Create regular COUNTER metric
    MetricEntity counterEntity =
        createThreeEnumMetricEntity("test_counter_concurrent", MetricType.COUNTER, "Test counter concurrent");
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> counterMetric =
        MetricEntityStateThreeEnums.create(
            counterEntity,
            otelRepository,
            baseDimensions,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Accumulators for timing (one per thread to avoid contention)
    long[] asyncTimes = new long[numThreads];
    long[] counterTimes = new long[numThreads];

    // Warmup
    LOGGER.info("Warming up with {} threads...", numThreads);
    Thread[] warmupThreads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      warmupThreads[t] = new Thread(() -> {
        for (int i = 0; i < numLoopsForWarmUp / numThreads; i++) {
          HttpResponseStatusEnum status = STATUS_ENUMS[ThreadLocalRandom.current().nextInt(STATUS_ENUMS.length)];
          HttpResponseStatusCodeCategory category =
              CATEGORY_ENUMS[ThreadLocalRandom.current().nextInt(CATEGORY_ENUMS.length)];
          VeniceResponseStatusCategory veniceCategory =
              RESPONSE_CATEGORIES[ThreadLocalRandom.current().nextInt(RESPONSE_CATEGORIES.length)];
          asyncCounterMetric.record(1L, status, category, veniceCategory);
          counterMetric.record(1L, status, category, veniceCategory);
        }
      });
      warmupThreads[t].start();
    }
    for (Thread t: warmupThreads) {
      t.join();
    }

    // Test both metrics with same threads, interleaved inner loops
    LOGGER.info("Testing ASYNC_COUNTER_FOR_HIGH_PERF_CASES vs COUNTER with {} threads (interleaved)...", numThreads);
    int numOuterLoops = numLoopsPerThread / numInternalLoops;

    Thread[] testThreads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      final int threadIdx = t;
      testThreads[t] = new Thread(() -> {
        long localAsyncTime = 0;
        long localCounterTime = 0;

        for (int i = 0; i < numOuterLoops; i++) {
          HttpResponseStatusEnum status = STATUS_ENUMS[i % STATUS_ENUMS.length];
          HttpResponseStatusCodeCategory category = CATEGORY_ENUMS[i % CATEGORY_ENUMS.length];
          VeniceResponseStatusCategory veniceCategory = RESPONSE_CATEGORIES[i % RESPONSE_CATEGORIES.length];

          // Measure ASYNC_COUNTER_FOR_HIGH_PERF_CASES
          long startAsync = System.nanoTime();
          for (int j = 0; j < numInternalLoops; j++) {
            asyncCounterMetric.record(1L, status, category, veniceCategory);
          }
          localAsyncTime += (System.nanoTime() - startAsync);

          // Measure regular COUNTER
          long startCounter = System.nanoTime();
          for (int j = 0; j < numInternalLoops; j++) {
            counterMetric.record(1L, status, category, veniceCategory);
          }
          localCounterTime += (System.nanoTime() - startCounter);
        }

        asyncTimes[threadIdx] = localAsyncTime;
        counterTimes[threadIdx] = localCounterTime;
      });
      testThreads[t].start();
    }
    for (Thread t: testThreads) {
      t.join();
    }

    // Sum up the times from all threads
    long asyncCounterDuration = 0;
    long counterDuration = 0;
    for (int t = 0; t < numThreads; t++) {
      asyncCounterDuration += asyncTimes[t];
      counterDuration += counterTimes[t];
    }

    LOGGER.info("=== Concurrent Performance Comparison: {} threads ===", numThreads);
    int totalOperations = numLoops * numInternalLoops;
    printBenchmarkResults(
        "ASYNC_COUNTER_FOR_HIGH_PERF_CASES (LongAdder)",
        asyncCounterDuration,
        "Regular COUNTER",
        counterDuration,
        totalOperations);
    assertTrue(asyncCounterDuration < counterDuration, "Async counter should be faster than regular counter");
  }

  /**
   * Multi-threaded performance comparison of LongAdder vs AtomicLong.
   * LongAdder should be significantly faster under high contention due to its
   * cell-based design that reduces contention across threads.
   */
  @Test
  public void testConcurrentLongAdderVsAtomicLong() throws InterruptedException {
    int numThreads = 8;
    int numLoopsForWarmUp = 1000;
    int numLoopsPerThread = 10000000;
    int numInternalLoops = 100000;

    LongAdder longAdder = new LongAdder();
    AtomicLong atomicLong = new AtomicLong(0);

    // Accumulators for timing (one per thread to avoid contention)
    long[] longAdderTimes = new long[numThreads];
    long[] atomicLongTimes = new long[numThreads];

    // Warmup
    LOGGER.info("Warming up LongAdder vs AtomicLong with {} threads...", numThreads);
    Thread[] warmupThreads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      warmupThreads[t] = new Thread(() -> {
        for (int i = 0; i < numLoopsForWarmUp / numThreads; i++) {
          longAdder.add(1);
          atomicLong.incrementAndGet();
        }
      });
      warmupThreads[t].start();
    }
    for (Thread t: warmupThreads) {
      t.join();
    }

    // Reset counters after warmup
    longAdder.reset();
    atomicLong.set(0);

    // Test both with same threads, interleaved inner loops
    LOGGER.info("Testing LongAdder vs AtomicLong with {} threads (interleaved)...", numThreads);
    int numOuterLoops = numLoopsPerThread / numInternalLoops;

    Thread[] testThreads = new Thread[numThreads];
    for (int t = 0; t < numThreads; t++) {
      final int threadIdx = t;
      testThreads[t] = new Thread(() -> {
        long localLongAdderTime = 0;
        long localAtomicLongTime = 0;

        for (int i = 0; i < numOuterLoops; i++) {
          // Measure LongAdder
          long startLongAdder = System.nanoTime();
          for (int j = 0; j < numInternalLoops; j++) {
            longAdder.add(1);
          }
          localLongAdderTime += (System.nanoTime() - startLongAdder);

          // Measure AtomicLong
          long startAtomicLong = System.nanoTime();
          for (int j = 0; j < numInternalLoops; j++) {
            atomicLong.incrementAndGet();
          }
          localAtomicLongTime += (System.nanoTime() - startAtomicLong);
        }

        longAdderTimes[threadIdx] = localLongAdderTime;
        atomicLongTimes[threadIdx] = localAtomicLongTime;
      });
      testThreads[t].start();
    }
    for (Thread t: testThreads) {
      t.join();
    }

    // Sum up the times from all threads
    long totalLongAdderTime = 0;
    long totalAtomicLongTime = 0;
    for (int t = 0; t < numThreads; t++) {
      totalLongAdderTime += longAdderTimes[t];
      totalAtomicLongTime += atomicLongTimes[t];
    }

    // Verify correctness - both should have the same count
    long expectedCount = (long) numThreads * numLoopsPerThread;
    assertEquals(longAdder.sum(), expectedCount, "LongAdder count mismatch");
    assertEquals(atomicLong.get(), expectedCount, "AtomicLong count mismatch");

    LOGGER.info("=== Concurrent Performance Comparison: LongAdder vs AtomicLong ({} threads) ===", numThreads);
    int totalOperations = numThreads * numLoopsPerThread;
    printBenchmarkResults("LongAdder", totalLongAdderTime, "AtomicLong", totalAtomicLongTime, totalOperations);
    assertTrue(
        totalLongAdderTime < totalAtomicLongTime,
        "LongAdder should be faster than AtomicLong under high contention");
  }
}
