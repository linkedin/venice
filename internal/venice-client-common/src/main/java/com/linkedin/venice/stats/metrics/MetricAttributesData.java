package com.linkedin.venice.stats.metrics;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.atomic.LongAdder;


/**
 * A holder class that contains OpenTelemetry {@link Attributes} and optionally a {@link LongAdder}
 * for each of that attributes for high-throughput metric recording scenarios.
 *
 * <p>This class is used by {@link MetricEntityState} subclasses to cache both the attributes
 * and the accumulator for observable counter metrics ({@link MetricType#ASYNC_COUNTER_FOR_HIGH_PERF_CASES}
 * and {@link MetricType#ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES}). For other metric types,
 * only the attributes are used and the adder remains null.
 *
 * <p>The {@link LongAdder} provides high-throughput recording capability by minimizing contention
 * across threads. The accumulated value is read during OpenTelemetry's metric collection callback
 * via {@link #sum()} (for ObservableLongCounter/UpDownCounter callbacks that must report cumulative values).
 */
public class MetricAttributesData {
  private final Attributes attributes;
  private final LongAdder adder;

  /**
   * Creates a MetricAttributesData with only attributes (for non-observable-counter metrics).
   *
   * @param attributes the OpenTelemetry attributes for this metric dimension combination
   */
  public MetricAttributesData(Attributes attributes) {
    this(attributes, false);
  }

  /**
   * Creates a MetricAttributesData with attributes and optionally a LongAdder.
   *
   * @param attributes the OpenTelemetry attributes for this metric dimension combination
   * @param createAdder if true, creates a LongAdder for high-throughput recording
   */
  public MetricAttributesData(Attributes attributes, boolean createAdder) {
    this.attributes = attributes;
    this.adder = createAdder ? new LongAdder() : null;
  }

  /**
   * Returns the OpenTelemetry attributes.
   */
  public Attributes getAttributes() {
    return attributes;
  }

  /**
   * Returns whether this holder has a LongAdder for high-throughput recording.
   */
  public boolean hasAdder() {
    return adder != null;
  }

  /**
   * Adds the given value to the internal LongAdder.
   * This is a fast operation optimized for high contention scenarios.
   * Only call this for observable counter metrics where adder is guaranteed non-null.
   *
   * @param value the value to add
   */
  public void add(long value) {
    adder.add(value);
  }

  /**
   * Returns the current cumulative sum without resetting.
   * This is the correct method to use in OpenTelemetry's ObservableLongCounter/UpDownCounter callbacks,
   * which must report <b>cumulative</b> values per the OTel spec. The SDK handles delta computation
   * internally based on the configured aggregation temporality.
   *
   * <p>Using {@code sumThenReset()} in observable counter callbacks causes the SDK to compute
   * delta-of-delta (because it subtracts the previous observation from the current one, but
   * {@code sumThenReset()} already returns a delta), producing negative counter values when
   * traffic varies between collection intervals.
   *
   * @return the current cumulative sum
   */
  public long sum() {
    return adder.sum();
  }

}
