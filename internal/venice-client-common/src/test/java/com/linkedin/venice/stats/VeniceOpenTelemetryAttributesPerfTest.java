package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCode.transformHttpResponseStatusToHttpResponseStatusCode;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.dimensions.HttpResponseStatusCode;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class VeniceOpenTelemetryAttributesPerfTest {
  // Marking this as flaky as we don't want to run this test in every build.
  @Test(groups = "flaky")
  public void testGeneratingAttributes() {
    int numStores = 500;
    List<MetricEntityStateThreeEnums<HttpResponseStatusCode, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory>> metricList =
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

    for (int i = 0; i < numStores; i++) {
      baseMetricDimensionsMap.put(VeniceMetricsDimensions.VENICE_STORE_NAME, "test_store_medium_sized_name" + i);
      metricList.add(
          MetricEntityStateThreeEnums.create(
              metricEntity,
              otelRepository,
              baseMetricDimensionsMap,
              HttpResponseStatusCode.class,
              HttpResponseStatusCodeCategory.class,
              VeniceResponseStatusCategory.class));
    }

    // Configuration
    int iterations = 100000000;
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
    System.out.println(
        "JVM/JDK: " + System.getProperty("java.runtime.name") + " " + System.getProperty("java.runtime.version"));

    // Start test
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      int j = RandomGenUtils.getRandomIntWithin(metricList.size());
      MetricEntityStateThreeEnums<HttpResponseStatusCode, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> metricEntityState =
          metricList.get(j);
      j = RandomGenUtils.getRandomIntWithin(possibleStatuses.length);
      HttpResponseStatus httpResponseStatus = possibleStatuses[j];
      HttpResponseStatusCode httpResponseStatusCode =
          transformHttpResponseStatusToHttpResponseStatusCode(httpResponseStatus);
      HttpResponseStatusCodeCategory httpResponseStatusCodeCategory =
          getVeniceHttpResponseStatusCodeCategory(httpResponseStatus);
      j = RandomGenUtils.getRandomIntWithin(responseCategories.length);
      VeniceResponseStatusCategory veniceResponseStatusCategory = responseCategories[j];
      Attributes attributes = metricEntityState
          .getAttributes(httpResponseStatusCode, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
      assertEquals(attributes.size(), 6);
    }
    // end test
    long endTime = System.currentTimeMillis();
    System.out.println("Total time taken: " + (endTime - startTime) + " ms");
    System.out.println("Number of loops: " + formatNumber(iterations));
    System.out.println("Average time per loop: " + formatNumber((int) ((endTime - startTime) / iterations)) + " ms");

    for (GarbageCollectorMXBean gcBean: ManagementFactory.getGarbageCollectorMXBeans()) {
      System.out.println(
          "Garbage Collector: " + gcBean.getName() + ", Collections: " + gcBean.getCollectionCount() + ", Time: "
              + gcBean.getCollectionTime() + " ms");
    }
  }

  private String formatNumber(int number) {
    return NumberFormat.getNumberInstance(Locale.US).format(number);
  }
}
