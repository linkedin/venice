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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class VeniceOpenTelemetryPerfTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryPerfTest.class);

  private static String formatNumber(long number) {
    return NumberFormat.getNumberInstance(Locale.US).format(number);
  }

  private static Map<VeniceMetricsDimensions, String> createBaseDimensions() {
    Map<VeniceMetricsDimensions, String> dimensions = new HashMap<>();
    dimensions.put(VeniceMetricsDimensions.VENICE_STORE_NAME, "test_store");
    dimensions.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, "test_cluster");
    return dimensions;
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
    LOGGER.info(
        String.format(
            "Comparison: %s is %.2fx %s than %s",
            approach1Name,
            Math.abs(ratio),
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
    Map<VeniceMetricsDimensions, String> baseDimensions = createBaseDimensions();
    baseDimensions.put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, "multi_get_streaming");

    MetricEntity callCountMetric = new MetricEntity(
        "call_count",
        MetricType.HISTOGRAM,
        MetricUnit.NUMBER,
        "Histogram",
        Utils.setOf(
            VeniceMetricsDimensions.VENICE_STORE_NAME,
            VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
            VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY));

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

    // Pre-generate dimension combinations
    HttpResponseStatusEnum[] statusEnums = { HttpResponseStatusEnum.OK, HttpResponseStatusEnum.BAD_REQUEST,
        HttpResponseStatusEnum.INTERNAL_SERVER_ERROR, HttpResponseStatusEnum.NOT_FOUND };
    HttpResponseStatusCodeCategory[] statusCategories = { HttpResponseStatusCodeCategory.SUCCESS,
        HttpResponseStatusCodeCategory.CLIENT_ERROR, HttpResponseStatusCodeCategory.SERVER_ERROR };
    VeniceResponseStatusCategory[] veniceCategories = VeniceResponseStatusCategory.values();

    LOGGER.info("Starting benchmark with " + formatNumber(numLoops) + " loops...");

    // Warmup
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      HttpResponseStatusEnum status = statusEnums[i % statusEnums.length];
      HttpResponseStatusCodeCategory category = statusCategories[i % statusCategories.length];
      VeniceResponseStatusCategory veniceCategory = veniceCategories[i % veniceCategories.length];
      metricOtelOnly.record(1L, status, category, veniceCategory);
      metricTehutiOnly.record(1L, status, category, veniceCategory);
    }

    long totalOtelTimeNs = 0;
    long totalTehutiTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = statusEnums[i % statusEnums.length];
      HttpResponseStatusCodeCategory category = statusCategories[i % statusCategories.length];
      VeniceResponseStatusCategory veniceCategory = veniceCategories[i % veniceCategories.length];

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
    assertTrue(totalOtelTimeNs < totalTehutiTimeNs, "OpenTelemetry should be faster than Tehuti");
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
}
