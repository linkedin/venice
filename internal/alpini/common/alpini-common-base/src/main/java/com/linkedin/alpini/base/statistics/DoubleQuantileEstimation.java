package com.linkedin.alpini.base.statistics;

import java.util.List;
import java.util.function.DoubleConsumer;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class DoubleQuantileEstimation extends AbstractQuantileEstimation<DoubleQuantileEstimation.Sample>
    implements DoubleConsumer {
  public DoubleQuantileEstimation(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  @Override
  protected int compare(Sample o1, Sample o2) {
    return Double.compare(o1._value, o2._value);
  }

  public Quantile computeQuantile(double v) {
    return super.computeQuantile(new Sample(v));
  }

  public double query(double quantile) {
    List<Sample> sample = querySample(new Quantiles(quantile));
    return !sample.isEmpty() ? sample.iterator().next()._value : Double.NaN;
  }

  public final double[] query(@Nonnull Quantiles quantiles) {
    return querySample(quantiles).stream().mapToDouble(Sample::value).toArray();
  }

  @Override
  public void accept(double v) {
    if (Double.isFinite(v)) {
      accept(new Sample(v));
    }
  }

  protected static final class Sample extends AbstractQuantileEstimation.AbstractSample<Sample> {
    protected final double _value;

    private Sample(double value) {
      _value = value;
    }

    public double value() {
      return _value;
    }

    @Override
    public String toString() {
      return String.valueOf(value());
    }
  }
}
