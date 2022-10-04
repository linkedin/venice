package com.linkedin.alpini.base.statistics;

import java.util.List;
import java.util.function.LongConsumer;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LongQuantileEstimation extends AbstractQuantileEstimation<LongQuantileEstimation.Sample>
    implements LongConsumer {
  public LongQuantileEstimation(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  @Override
  protected final int compare(Sample o1, Sample o2) {
    return Long.compare(o1.value(), o2.value());
  }

  public Quantile computeQuantile(long v) {
    return super.computeQuantile(new Sample(v));
  }

  public final Long query(double quantile) {
    List<Sample> sample = querySample(new Quantiles(quantile));
    return !sample.isEmpty() ? sample.iterator().next()._value : null;
  }

  public final long[] query(@Nonnull Quantiles quantiles) {
    return querySample(quantiles).stream().mapToLong(Sample::value).toArray();
  }

  @Override
  public void accept(long v) {
    accept(new Sample(v));
  }

  protected static final class Sample extends AbstractQuantileEstimation.AbstractSample<Sample> {
    protected final long _value;

    private Sample(long value) {
      _value = value;
    }

    public final long value() {
      return _value;
    }

    @Override
    public String toString() {
      return String.valueOf(value());
    }
  }
}
