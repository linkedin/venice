package com.linkedin.alpini.base.statistics;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class GenericQuantileEstimation<T> extends AbstractQuantileEstimation<GenericQuantileEstimation.Sample<T>>
    implements Consumer<T> {
  private final Comparator<T> _comparator;

  public GenericQuantileEstimation(double epsilon, int compactSize, Comparator<T> comparator) {
    super(epsilon, compactSize);
    _comparator = Objects.requireNonNull(comparator);
  }

  @Override
  protected final int compare(Sample<T> o1, Sample<T> o2) {
    return _comparator.compare(o1.value(), o2.value());
  }

  public Quantile computeQuantile(@Nonnull T v) {
    return super.computeQuantile(new Sample<>(v));
  }

  public T query(double quantile) {
    List<Sample<T>> sample = querySample(new Quantiles(quantile));
    return !sample.isEmpty() ? sample.iterator().next().value() : null;
  }

  @Override
  public void accept(T v) {
    accept(newSample(v));
  }

  protected Sample<T> newSample(T value) {
    return new Sample<>(value);
  }

  protected static class Sample<T> extends AbstractQuantileEstimation.AbstractSample<Sample<T>> {
    public @Nonnull T _value;

    public Sample(@Nonnull T value) {
      _value = Objects.requireNonNull(value);
    }

    public final T value() {
      return _value;
    }

    @Override
    public String toString() {
      return String.valueOf(value());
    }
  }
}
