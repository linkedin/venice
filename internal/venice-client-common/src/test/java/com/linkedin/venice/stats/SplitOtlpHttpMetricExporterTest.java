package com.linkedin.venice.stats;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SplitOtlpHttpMetricExporterTest {
  private MetricExporter delegate;
  private SplitOtlpHttpMetricExporter exporter;
  private MetricData metric;

  @BeforeMethod
  public void setUp() {
    // Mock the delegate exporter and a single MetricData
    delegate = mock(MetricExporter.class);
    exporter = new SplitOtlpHttpMetricExporter(delegate, 3);
    metric = mock(MetricData.class);
    Data<PointData> data = mock(Data.class);
    doReturn(data).when(metric).getData();
  }

  @Test
  public void testExportSmallerThanBatchSize() {
    // given a metric with 2 points (< maxBatchSize)
    Collection<PointData> points = Arrays.asList(mock(PointData.class), mock(PointData.class));
    Data data = metric.getData();
    doReturn(points).when(data).getPoints();
    when(delegate.export(anyCollection())).thenReturn(CompletableResultCode.ofSuccess());

    // when exporting
    CompletableResultCode result = exporter.export(Collections.singletonList(metric));

    // then delegate.export called once with the full batch and overall result is success
    verify(delegate, times(1)).export(anyCollection());
    assertTrue(result.isSuccess());
  }

  @Test
  public void testExportLargerThanBatchSize() {
    // given a metric with 10 points (> maxBatchSize)
    List<PointData> points = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      points.add(mock(PointData.class));
    }
    Data data = metric.getData();
    doReturn(points).when(data).getPoints();
    when(delegate.export(anyCollection())).thenReturn(CompletableResultCode.ofSuccess());

    // when exporting
    CompletableResultCode result = exporter.export(Collections.singletonList(metric));

    // then delegate.export called 4 times (3 full chunks + 1 remainder) and overall result is success
    verify(delegate, times(4)).export(anyCollection());
    assertTrue(result.isSuccess());
  }
}
