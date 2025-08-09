package com.linkedin.venice.stats;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.data.SummaryData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * A decorator for {@link MetricData} that restricts the range of points returned
 * based on the specified start and end indices.
 *
 * <p>This allows selective access to a subset of data points within a given metric.
 * The decorator ensures that only points within the specified index range are accessible.
 *
 * <p>Usage Example:
 * <pre>
 * MetricData originalMetricData = ...;
 * MetricDataRangeDecorator decorator = new MetricDataRangeDecorator(originalMetricData, 2, 5);
 * Collection<PointData> filteredPoints = decorator.getData().getPoints();
 * </pre>
 *
 * @param <T> The type of {@link PointData} contained in the metric data.
 */
public class MetricDataRangeDecorator<T extends PointData> implements MetricData {
  private final MetricData originalMetricData;
  private final int startIndex;
  private final int endIndex;

  public MetricDataRangeDecorator(MetricData originalMetricData, int startIndex, int endIndex) {
    if (startIndex < 0 || endIndex > originalMetricData.getData().getPoints().size() || startIndex > endIndex) {
      throw new IllegalArgumentException("Invalid range specified");
    }
    this.originalMetricData = originalMetricData;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  @Override
  public MetricDataType getType() {
    return originalMetricData.getType();
  }

  @Override
  public String getName() {
    return originalMetricData.getName();
  }

  @Override
  public String getDescription() {
    return originalMetricData.getDescription();
  }

  @Override
  public String getUnit() {
    return originalMetricData.getUnit();
  }

  @Override
  public Resource getResource() {
    return originalMetricData.getResource();
  }

  @Override
  public InstrumentationScopeInfo getInstrumentationScopeInfo() {
    return originalMetricData.getInstrumentationScopeInfo();
  }

  /**
   * Returns the metric data for the specified type.
   *
   * <p>This method decorates the original metric data with a range filter applied to the points.
   * It ensures that the returned data is of the correct type and can be cast to the expected type
   * in the calling code.
   *
   * @return the metric data with the range filter applied.
   * @throws ClassCastException if the original metric data is not of the expected type.
   */
  @Override
  public Data<?> getData() {
    Data<?> originalData = originalMetricData.getData();

    switch (originalMetricData.getType()) {
      case EXPONENTIAL_HISTOGRAM:
        return new ExponentialHistogramData() {
          @Override
          public AggregationTemporality getAggregationTemporality() {
            return ((ExponentialHistogramData) originalData).getAggregationTemporality();
          }

          @Override
          public Collection<ExponentialHistogramPointData> getPoints() {
            return createPointCollection((ExponentialHistogramData) originalData);
          }
        };

      case DOUBLE_GAUGE:
        return new GaugeData<DoublePointData>() {
          @Override
          public Collection<DoublePointData> getPoints() {
            return createPointCollection((GaugeData<DoublePointData>) originalData);
          }
        };

      case LONG_GAUGE:
        return new GaugeData<LongPointData>() {
          @Override
          public Collection<LongPointData> getPoints() {
            return createPointCollection((GaugeData<LongPointData>) originalData);
          }
        };

      case DOUBLE_SUM:
        return new SumData<DoublePointData>() {
          @Override
          public AggregationTemporality getAggregationTemporality() {
            return ((ImmutableSumData<DoublePointData>) originalData).getAggregationTemporality();
          }

          @Override
          public Collection<DoublePointData> getPoints() {
            return createPointCollection((ImmutableSumData<DoublePointData>) originalData);
          }

          @Override
          public boolean isMonotonic() {
            return ((ImmutableSumData<DoublePointData>) originalData).isMonotonic();
          }
        };

      case LONG_SUM:
        return new SumData<LongPointData>() {
          @Override
          public AggregationTemporality getAggregationTemporality() {
            return ((ImmutableSumData<LongPointData>) originalData).getAggregationTemporality();
          }

          @Override
          public Collection<LongPointData> getPoints() {
            return createPointCollection((ImmutableSumData<LongPointData>) originalData);
          }

          @Override
          public boolean isMonotonic() {
            return ((ImmutableSumData<LongPointData>) originalData).isMonotonic();
          }
        };

      case HISTOGRAM:
        return new HistogramData() {
          @Override
          public AggregationTemporality getAggregationTemporality() {
            return ((HistogramData) originalData).getAggregationTemporality();
          }

          @Override
          public Collection<HistogramPointData> getPoints() {
            return createPointCollection((HistogramData) originalData);
          }
        };

      case SUMMARY:
        return new SummaryData() {
          @Override
          public Collection<SummaryPointData> getPoints() {
            return createPointCollection((SummaryData) originalData);
          }
        };

      default:
        throw new IllegalStateException("Unsupported MetricData type: " + originalMetricData.getType());
    }
  }

  /**
   * Override the open source method only to help type cast to SumData.
   */
  @Override
  public SumData<DoublePointData> getDoubleSumData() {
    if (getType() == MetricDataType.DOUBLE_SUM) {
      return (SumData<DoublePointData>) getData();
    }
    return ImmutableSumData.empty();
  }

  /**
   * Creates a collection that wraps the original points and applies the range filter.
   */
  private <T extends PointData> Collection<T> createPointCollection(Data<T> originalData) {
    return new AbstractCollection<T>() {
      @Override
      public Iterator<T> iterator() {
        return new RangeIterator<>(originalData.getPoints().iterator(), startIndex, endIndex);
      }

      @Override
      public int size() {
        return Math.max(0, endIndex - startIndex);
      }
    };
  }

  /**
   * Compares this {@link MetricDataRangeDecorator} with the specified object for equality.
   *
   * <p>This method performs a one-way equality check:
   * <ul>
   *   <li>If the object is the same instance, it returns {@code true}.</li>
   *   <li>If the object is a {@link MetricDataRangeDecorator}, it compares the underlying
   *       {@link MetricData} and the start and end indices of both decorators.</li>
   *   <li>If the object is a plain {@link MetricData}, it is considered equal only if the
   *       decorator represents the full range (start index = 0, end index = size of points)
   *       and the underlying {@link MetricData} is equal.</li>
   * </ul>
   *
   * <p><b>Note:</b> This equality comparison works only one way:
   * - {@code decorator.equals(original)} will return {@code true} if the decorator covers the full range.
   * - {@code original.equals(decorator)} will always return {@code false}.
   *
   * @param obj the object to compare this decorator with
   * @return {@code true} if the object is equal to this decorator; {@code false} otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof MetricDataRangeDecorator) {
      MetricDataRangeDecorator<?> other = (MetricDataRangeDecorator<?>) obj;
      return this.startIndex == other.startIndex && this.endIndex == other.endIndex
          && this.originalMetricData.equals(other.originalMetricData);
    }
    return this.startIndex == 0 && this.endIndex == originalMetricData.getData().getPoints().size()
        && originalMetricData.equals(obj);
  }

  @Override
  public int hashCode() {
    if (startIndex == 0 && endIndex == originalMetricData.getData().getPoints().size()) {
      return originalMetricData.hashCode();
    }
    return 31 * originalMetricData.hashCode() + startIndex + endIndex;
  }

  @Override
  public String toString() {
    return "MetricDataRangeDecorator{" + "originalMetricData=" + originalMetricData + ", startIndex=" + startIndex
        + ", endIndex=" + endIndex + '}';
  }

  /**
   * An iterator that applies a range filter to the original iterator.
  */
  private static class RangeIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private final int endIndex;
    private int currentIndex = 0;

    RangeIterator(Iterator<T> iterator, int startIndex, int endIndex) {
      this.iterator = iterator;
      this.endIndex = endIndex;

      // Skip elements until reaching startIndex
      while (currentIndex < startIndex && iterator.hasNext()) {
        iterator.next();
        currentIndex++;
      }
    }

    @Override
    public boolean hasNext() {
      return currentIndex < endIndex && iterator.hasNext();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      currentIndex++;
      return iterator.next();
    }
  }
}
