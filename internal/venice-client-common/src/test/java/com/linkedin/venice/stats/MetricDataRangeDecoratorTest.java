package com.linkedin.venice.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricDataRangeDecoratorTest {
  private MetricData original;
  private List<PointData> originalPoints;

  @BeforeMethod
  public void setUp() {
    // Mock an original MetricData with 5 dummy points
    original = mock(MetricData.class);
    originalPoints = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      originalPoints.add(mock(PointData.class));
    }
    GaugeData<PointData> data = mock(GaugeData.class);
    when(data.getPoints()).thenReturn(originalPoints);
    when(original.getType()).thenReturn(MetricDataType.DOUBLE_GAUGE);
    doReturn(data).when(original).getData();
  }

  @Test
  public void testGetDataPointsRange() {
    // when selecting range [1,4)
    MetricDataRangeDecorator<PointData> decorator = new MetricDataRangeDecorator<>(original, 1, 4);
    GaugeData<PointData> decoratedData = (GaugeData<PointData>) decorator.getData();
    Collection<PointData> filtered = decoratedData.getPoints();

    // then only points at indices 1,2,3 are returned
    assertEquals(filtered.size(), 3);
    Iterator<PointData> it = filtered.iterator();
    assertSame(it.next(), originalPoints.get(1));
    assertSame(it.next(), originalPoints.get(2));
    assertSame(it.next(), originalPoints.get(3));
  }

  @Test
  public void testEqualsAndHashCode() {
    // full-range decorator should equal the original and share hashCode
    MetricDataRangeDecorator<PointData> fullDecorator =
        new MetricDataRangeDecorator<>(original, 0, originalPoints.size());
    assertTrue(fullDecorator.equals(original));
    assertEquals(fullDecorator.hashCode(), original.hashCode());

    // partial-range decorator should not equal the original
    MetricDataRangeDecorator<PointData> partialDecorator = new MetricDataRangeDecorator<>(original, 1, 4);
    assertFalse(partialDecorator.equals(original));
    // but should equal another decorator with the same range & underlying
    assertTrue(partialDecorator.equals(new MetricDataRangeDecorator<>(original, 1, 4)));
    // and its hashCode follows: 31*origHash + startIndex + endIndex
    int expectedHash = 31 * original.hashCode() + 1 + 4;
    assertEquals(partialDecorator.hashCode(), expectedHash);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidRangeThrows() {
    new MetricDataRangeDecorator<>(original, 4, 1);
  }
}
