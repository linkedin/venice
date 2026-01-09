package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformHttpResponseStatusToHttpResponseStatusEnum;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
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
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceOpenTelemetryPerfTest {
  private final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryPerfTest.class);

  // Marking this as flaky as we don't want to run this test in every build.
  @Test(groups = "flaky")
  public void testGeneratingAttributes() {
    // config
    boolean createAttributes = true;
    int numStores = 500;
    int iterations = 1000000000;

    List<MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory>> metricList =
        new ArrayList<>();
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(true);
    when(mockMetricsConfig.getMetricNamingFormat()).thenReturn(VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE);
    VeniceOpenTelemetryMetricsRepository otelRepository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
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

    // Print JVM/JDK information
    printJvmInfo();
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

    // Start test
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
    // end test
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

    for (GarbageCollectorMXBean gcBean: ManagementFactory.getGarbageCollectorMXBeans()) {
      LOGGER.info(
          "Garbage Collector: " + gcBean.getName() + ", Collections: " + gcBean.getCollectionCount() + ", Time: "
              + gcBean.getCollectionTime() + " ms");
    }
  }

  private String formatNumber(int number) {
    return NumberFormat.getNumberInstance(Locale.US).format(number);
  }

  private String formatNumber(long number) {
    return NumberFormat.getNumberInstance(Locale.US).format(number);
  }

  private static class TestFixtures {
    final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricOtelOnly;
    final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricTehutiOnly;
    final HttpResponseStatusEnum[] statusEnums;
    final HttpResponseStatusCodeCategory[] statusCategories;
    final VeniceResponseStatusCategory[] veniceCategories;

    TestFixtures(
        MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricOtelOnly,
        MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricTehutiOnly,
        HttpResponseStatusEnum[] statusEnums,
        HttpResponseStatusCodeCategory[] statusCategories,
        VeniceResponseStatusCategory[] veniceCategories) {
      this.metricOtelOnly = metricOtelOnly;
      this.metricTehutiOnly = metricTehutiOnly;
      this.statusEnums = statusEnums;
      this.statusCategories = statusCategories;
      this.veniceCategories = veniceCategories;
    }
  }

  private TestFixtures createTestFixtures() {
    // Setup OpenTelemetry repository
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(true);
    when(mockMetricsConfig.getMetricNamingFormat()).thenReturn(VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE);
    VeniceOpenTelemetryMetricsRepository otelRepository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    // Setup base dimensions
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_STORE_NAME, "test_store");
    baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, "test_cluster");
    baseDimensionsMap.put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, "multi_get_streaming");

    // Create MetricEntity
    MetricEntity callCountMetric = new MetricEntity(
        "call_count",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Count of all requests during response handling along with response codes",
        Utils.setOf(
            VeniceMetricsDimensions.VENICE_STORE_NAME,
            VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
            VeniceMetricsDimensions.VENICE_REQUEST_METHOD,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE,
            VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY));

    // Create metric with only OpenTelemetry
    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricOtelOnly =
        MetricEntityStateThreeEnums.create(
            callCountMetric,
            otelRepository,
            baseDimensionsMap,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Create metric with only Tehuti
    MetricsRepository tehutiOnlyRepository = new MetricsRepository();
    List<MeasurableStat> tehutiOnlyStats = new ArrayList<>();
    tehutiOnlyStats.add(new OccurrenceRate());
    tehutiOnlyStats.add(new Rate());

    MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricTehutiOnly =
        MetricEntityStateThreeEnums.create(
            callCountMetric,
            null, // No OTel repository
            (name, stats) -> {
              Sensor sensor = tehutiOnlyRepository.sensor(name);
              for (MeasurableStat stat: stats) {
                sensor.add(name + "." + stat.getClass().getSimpleName(), stat);
              }
              return sensor;
            },
            new TehutiMetricNameEnum() {
              @Override
              public String getMetricName() {
                return "call_count_tehuti_only";
              }
            },
            tehutiOnlyStats,
            baseDimensionsMap,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            VeniceResponseStatusCategory.class);

    // Pre-generate random dimension combinations for consistent testing
    HttpResponseStatusEnum[] statusEnums = { HttpResponseStatusEnum.OK, HttpResponseStatusEnum.BAD_REQUEST,
        HttpResponseStatusEnum.INTERNAL_SERVER_ERROR, HttpResponseStatusEnum.NOT_FOUND };

    HttpResponseStatusCodeCategory[] statusCategories = { HttpResponseStatusCodeCategory.SUCCESS,
        HttpResponseStatusCodeCategory.CLIENT_ERROR, HttpResponseStatusCodeCategory.SERVER_ERROR };

    VeniceResponseStatusCategory[] veniceCategories = VeniceResponseStatusCategory.values();

    return new TestFixtures(metricOtelOnly, metricTehutiOnly, statusEnums, statusCategories, veniceCategories);
  }

  private void warmupMetrics(TestFixtures fixtures, int numLoopsForWarmUp) {
    LOGGER.info("Warming up...");
    for (int i = 0; i < numLoopsForWarmUp; i++) {
      HttpResponseStatusEnum status = fixtures.statusEnums[i % fixtures.statusEnums.length];
      HttpResponseStatusCodeCategory category = fixtures.statusCategories[i % fixtures.statusCategories.length];
      VeniceResponseStatusCategory veniceCategory = fixtures.veniceCategories[i % fixtures.veniceCategories.length];
      fixtures.metricOtelOnly.record(1L, status, category, veniceCategory);
      fixtures.metricTehutiOnly.record(1L, status, category, veniceCategory);
    }
    LOGGER.info("Warm up completed.");
  }

  private void printResults(long otelDurationNs, long tehutiDurationNs, int numLoops) {
    LOGGER.info("========== Results ==========");
    LOGGER.info("OpenTelemetry Metrics:");
    LOGGER.info("  Total time: " + formatNumber((int) (otelDurationNs / 1_000_000)) + " ms");
    LOGGER.info("  Average time per record: " + String.format("%.2f", (double) otelDurationNs / numLoops) + " ns");
    LOGGER.info("  Records per second: " + formatNumber((long) numLoops * 1_000_000_000L / otelDurationNs));

    LOGGER.info("Tehuti Metrics:");
    LOGGER.info("  Total time: " + formatNumber((int) (tehutiDurationNs / 1_000_000)) + " ms");
    LOGGER.info("  Average time per record: " + String.format("%.2f", (double) tehutiDurationNs / numLoops) + " ns");
    LOGGER.info("  Records per second: " + formatNumber((long) numLoops * 1_000_000_000L / tehutiDurationNs));

    LOGGER.info("Comparison:");
    double ratio = (double) otelDurationNs / tehutiDurationNs;
    LOGGER.info(
        "  OpenTelemetry is " + String.format("%.2fx", ratio) + (ratio > 1 ? " slower" : " faster") + " than Tehuti");
    LOGGER.info("  Difference: " + formatNumber((int) Math.abs(otelDurationNs - tehutiDurationNs) / 1_000_000) + " ms");
  }

  private void printGCStats() {
    LOGGER.info("========== Garbage Collection Stats ==========");
    for (GarbageCollectorMXBean gcBean: ManagementFactory.getGarbageCollectorMXBeans()) {
      LOGGER.info("  " + gcBean.getName() + ":");
      LOGGER.info("    Collections: " + gcBean.getCollectionCount());
      LOGGER.info("    Time: " + gcBean.getCollectionTime() + " ms");
    }
  }

  private void printJvmInfo() {
    LOGGER
        .info("JVM/JDK: " + System.getProperty("java.runtime.name") + " " + System.getProperty("java.runtime.version"));
  }

  private long runMetricBenchmark(
      TestFixtures fixtures,
      int numLoops,
      MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metric) {
    long startTime = System.nanoTime();
    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = fixtures.statusEnums[i % fixtures.statusEnums.length];
      HttpResponseStatusCodeCategory category = fixtures.statusCategories[i % fixtures.statusCategories.length];
      VeniceResponseStatusCategory veniceCategory = fixtures.veniceCategories[i % fixtures.veniceCategories.length];
      metric.record(1L, status, category, veniceCategory);
    }
    long endTime = System.nanoTime();
    return endTime - startTime;
  }

  /**
   * Records OpenTelemetry and Tehuti metrics in separate loop iterations and measures their times separately
   * for comparison.
   */
  @Test(invocationCount = 10)
  public void testTehutiVsOtelMetricRecordingPerf() {
    int numLoops = 10000000; // 10M
    int numLoopsForWarmUp = 1000; // 1k

    TestFixtures fixtures = createTestFixtures();

    printJvmInfo();
    LOGGER.info("Number of loops: " + formatNumber(numLoops));

    warmupMetrics(fixtures, numLoopsForWarmUp);

    long otelDurationNs;
    long tehutiDurationNs;
    if (ThreadLocalRandom.current().nextInt(10) % 2 == 0) {
      // run otel first and then tehuti
      otelDurationNs = runMetricBenchmark(fixtures, numLoops, fixtures.metricOtelOnly);
      tehutiDurationNs = runMetricBenchmark(fixtures, numLoops, fixtures.metricTehutiOnly);
    } else {
      // run tehuti first and then otel
      tehutiDurationNs = runMetricBenchmark(fixtures, numLoops, fixtures.metricTehutiOnly);
      otelDurationNs = runMetricBenchmark(fixtures, numLoops, fixtures.metricOtelOnly);
    }

    printResults(otelDurationNs, tehutiDurationNs, numLoops);
    printGCStats();
    Assert.assertTrue(otelDurationNs < tehutiDurationNs, "OpenTelemetry should be faster than Tehuti");
  }

  /**
   * Records both OpenTelemetry and Tehuti metrics in the same loop iteration and measures their times separately
   * and aggregates the total times for comparison to reduce the JVM/JIT optimizations impact on the measurements.
   */
  @Test(invocationCount = 10)
  public void testTehutiVsOtelMetricRecordingPerfByRecordingBothMetricsInSameLoop() {
    int numLoops = 10000000; // 10M iterations
    int numLoopsForWarmUp = 1000; // 1k

    TestFixtures fixtures = createTestFixtures();

    printJvmInfo();
    LOGGER.info("Number of loops: " + formatNumber(numLoops));

    warmupMetrics(fixtures, numLoopsForWarmUp);

    long totalOtelTimeNs = 0;
    long totalTehutiTimeNs = 0;

    for (int i = 0; i < numLoops; i++) {
      HttpResponseStatusEnum status = fixtures.statusEnums[i % fixtures.statusEnums.length];
      HttpResponseStatusCodeCategory category = fixtures.statusCategories[i % fixtures.statusCategories.length];
      VeniceResponseStatusCategory veniceCategory = fixtures.veniceCategories[i % fixtures.veniceCategories.length];

      // Measure OpenTelemetry call
      long startOtel = System.nanoTime();
      fixtures.metricOtelOnly.record(1L, status, category, veniceCategory);
      long endOtel = System.nanoTime();
      totalOtelTimeNs += (endOtel - startOtel);

      // Measure Tehuti call
      long startTehuti = System.nanoTime();
      fixtures.metricTehutiOnly.record(1L, status, category, veniceCategory);
      long endTehuti = System.nanoTime();
      totalTehutiTimeNs += (endTehuti - startTehuti);
    }

    printResults(totalOtelTimeNs, totalTehutiTimeNs, numLoops);
    printGCStats();
    Assert.assertTrue(totalOtelTimeNs < totalTehutiTimeNs, "OpenTelemetry should be faster than Tehuti");
  }
}
