package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.concurrency.ConcurrentAccumulator;
import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.alpini.base.misc.Msg;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * A memory efficient abstract implementation for calculating quantile summaries.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 * @see "Greenwald and Khanna, 'Space-efficient online computation of quantile summaries' SIGMOD 2001"
 */
public abstract class AbstractQuantileEstimation<SAMPLE extends AbstractQuantileEstimation.AbstractSample<SAMPLE>> {
  protected final Logger _log = LogManager.getLogger(getClass());

  private final double _epsilon;
  private final int _compactSize;
  private final Collector<SAMPLE, Data, Data> _collector;
  private final Data _nullData;

  private final ConcurrentAccumulator<SAMPLE, Data, Data> _accumulator;

  protected AbstractQuantileEstimation(double epsilon, int compactSize) {
    this(epsilon, compactSize, ConcurrentAccumulator.defaultMode);
  }

  protected AbstractQuantileEstimation(double epsilon, int compactSize, ConcurrentAccumulator.Mode accumulatorMode) {
    _epsilon = epsilon;
    _compactSize = compactSize;
    _collector = collector();
    _accumulator = new ConcurrentAccumulator<>(accumulatorMode, _collector);
    _nullData = newData();
  }

  protected int getCompactSize() {
    return _compactSize;
  }

  protected abstract int compare(SAMPLE o1, SAMPLE o2);

  private final Comparator<SAMPLE> _comparator = (o1, o2) -> -compare(o1, o2);

  protected Collector<SAMPLE, Data, Data> collector() {
    return Collector.of(
        AbstractQuantileEstimation.this::newData,
        Data::accumulate,
        Data::combine,
        Function.identity(),
        Collector.Characteristics.UNORDERED,
        Collector.Characteristics.IDENTITY_FINISH);
  }

  protected @Nonnull Data newData() {
    return new Data();
  }

  private SortedSet<SAMPLE> newSortedSet() {
    return newSortedSet(_comparator);
  }

  protected SortedSet<SAMPLE> newSortedSet(Comparator<SAMPLE> comparator) {
    return new ConcurrentSkipListSet<>(comparator);
  }

  protected SortedSet<SAMPLE> cloneSortedSet(SortedSet<SAMPLE> samples) {
    return ((ConcurrentSkipListSet<SAMPLE>) samples).clone();
  }

  protected SAMPLE floor(SortedSet<SAMPLE> samples, SAMPLE v) {
    /* if (false) {
      // If there was no floor function
      for (SortedSet<SAMPLE> tailSet = samples.tailSet(v); !tailSet.isEmpty(); ) {
        SAMPLE test = tailSet.first();
        if (_comparator.compare(test, v) == 0) {
          return test;
        }
        break;
      }
      for (SortedSet<SAMPLE> headSet = samples.headSet(v); !headSet.isEmpty(); ) {
        return headSet.last();
      }
      return null;
    }*/
    return ((ConcurrentSkipListSet<SAMPLE>) samples).floor(v);
  }

  protected class Data {
    private SortedSet<SAMPLE> _samples = newSortedSet();

    /** number of samples received, long to avoid integer overflow */
    private long _count;

    public final @Nonnull List<SAMPLE> query(@Nonnull Quantiles quantile) {
      if (_samples.isEmpty()) {
        return Collections.emptyList();
      }

      // Create a list of samples in reverse order.
      LinkedList<SAMPLE> samples = new LinkedList<>();
      _samples.forEach(samples::addFirst);

      final int quantileCount = quantile.size();

      CollectionUtil.ListBuilder<SAMPLE> builder = CollectionUtil.listBuilder();

      int index = 0;
      long count = _count;
      double rankMin = 0;
      double[] desired = new double[quantileCount];

      for (int i = 0; i < quantileCount; i++) {
        desired[i] = ((quantile.get(i) + _epsilon * 2.0) * count);
      }

      final Iterator<SAMPLE> it = samples.iterator();
      SAMPLE prev;
      SAMPLE cur = it.next();

      out: while (index < quantileCount && it.hasNext()) {
        prev = cur;
        cur = it.next();
        rankMin += prev.g();

        while (rankMin + cur.error() > desired[index]) {
          builder.add(prev);
          if (++index == quantileCount) {
            break out;
          }
        }
      }
      while (index++ < quantileCount) {
        builder.add(cur);
      }
      return builder.build();
    }

    public Quantile computeQuantile(@Nonnull SAMPLE v) {
      if (_samples.isEmpty()) {
        return null;
      }

      // Create a list of samples in reverse order.
      LinkedList<SAMPLE> samples = new LinkedList<>();
      _samples.forEach(samples::addFirst);

      Iterator<SAMPLE> it = samples.iterator();
      if (it.hasNext()) {
        SAMPLE prev;
        SAMPLE cur = it.next();
        double rankMin = 0;
        while (it.hasNext()) {
          prev = cur;
          cur = it.next();
          rankMin += prev.g();
          if (_comparator.compare(v, cur) > 0) {
            double scale = 1.0 / _count;
            double quartile = (rankMin + cur.g() / 2) * scale - _epsilon * 2.0;
            double error = cur.error() * scale * 0.5;
            return new Quantile(Math.max(0.0, Math.min(quartile + error, 1.0)), error);
          }
        }
      }
      return new Quantile(1.0, 0.0);
    }

    void accumulate(SAMPLE v) {
      SAMPLE floor = floor(_samples, v);
      int delta = (int) Math.floor(2 * _epsilon * (_count + 1));
      if (floor == null || _comparator.compare(v, _samples.last()) > 0) {
        v.init(1, 0);
      } else {
        v.init(1, delta);
      }

      // As per §3, simplify tuple initialization and attempt to remove a tuple for every insert.
      // This is achieved by obtaining a lock on the the n+1 element (our set is reversed) and performing
      // the merge operation, removing the old element if it is to be replaced.
      if (floor == null || AbstractSample.error(v, floor) > delta) {
        _samples.add(v);
      } else {
        floor.merge(v, floor == _samples.first(), floor == _samples.last());
      }
      _count++;
    }

    Data combine(Data other) {
      if (_samples.isEmpty()) {
        _samples = cloneSortedSet(other._samples);
        _count = other._count;
        return this;
      }

      other._samples.forEach(v -> {
        SAMPLE floor = floor(_samples, v);
        int delta = (int) Math.floor(2 * _epsilon * (_count + v.count()));
        if (floor == null || AbstractSample.error(v, floor) > delta) {
          _samples.add(v.clone());
        } else {
          floor.merge(v, floor == _samples.first(), floor == _samples.last());
        }
        _count += v.count();
      });
      return this;
    }
  }

  protected final List<SAMPLE> querySample(@Nonnull Quantiles quantiles) {
    return data().query(Objects.requireNonNull(quantiles));
  }

  public void reset() {
    _accumulator.reset();
  }

  protected final @Nonnull Data data() {
    Data data = _accumulator.get();
    if (data == null) {
      data = _nullData;
    }
    return data;
  }

  protected @Nonnull List<SAMPLE> queryAndReset(@Nonnull Quantiles quantiles, Consumer<Data> consumer) {
    return Optional.ofNullable(_accumulator.getThenReset()).map(data -> {
      consumer.accept(data);
      return data;
    }).map(data -> data.query(Objects.requireNonNull(quantiles))).orElseGet(Collections::emptyList);
  }

  protected @Nonnull List<SAMPLE> queryAndReset(@Nonnull Quantiles quantiles) {
    return queryAndReset(quantiles, data -> {});
  }

  /**
   * Computes the quantile which the given sample {@literal v} is a member.
   * @param v sample
   * @return quantile
   */
  protected final Quantile computeQuantile(@Nonnull SAMPLE v) {
    return Optional.ofNullable(_accumulator.get()).map(data -> data.computeQuantile(v)).orElse(null);
  }

  public int getNumberOfSamples() {
    return data()._samples.size();
  }

  protected final void accept(SAMPLE v) {
    _accumulator.accept(Objects.requireNonNull(v));
  }

  private static double checkQuantile(double quantile) {
    if (quantile < 0.0 || quantile > 1.0) {
      throw new IllegalArgumentException("Quantile may only be between 0.0 and 1.0 inclusive");
    }
    return quantile;
  }

  public static final class Quantiles {
    private final @Nonnull double[] _quantiles;

    public Quantiles(double quantile) {
      _quantiles = new double[] { checkQuantile(quantile) };
    }

    public Quantiles(double first, double... quantiles) {
      if (quantiles.length > 0) {
        double current = checkQuantile(first);
        for (double quantile: quantiles) {
          if (current >= quantile) {
            throw new IllegalArgumentException("Quantiles must be specified in ascending order");
          }
          current = checkQuantile(quantile);
        }
      }
      _quantiles = new double[quantiles.length + 1];
      _quantiles[0] = first;
      System.arraycopy(quantiles, 0, _quantiles, 1, quantiles.length);
    }

    public int size() {
      return _quantiles.length;
    }

    public double get(int i) {
      return _quantiles[i];
    }
  }

  public static final class Quantile implements StringBuilderFormattable {
    private final double _quantile;
    private final double _error;

    public Quantile(double quantile, double error) {
      _quantile = quantile;
      _error = error;
    }

    public double getQuantile() {
      return _quantile;
    }

    public double getError() {
      return _error;
    }

    @Override
    public void formatTo(StringBuilder buffer) {
      buffer.append(toPct(getQuantile())).append("%ile ±").append(toPct(getError()));
    }

    private static double toPct(double value) {
      return Math.floor(value * 1000000.0) / 10000.0;
    }

    @Override
    public String toString() {
      StringBuilder builder = Msg.stringBuilder();
      formatTo(builder);
      return builder.toString();
    }
  }

  protected static abstract class AbstractSample<SAMPLE extends AbstractSample<SAMPLE>> implements Cloneable {
    private double _g;
    private int _delta;
    private int _count;

    public SAMPLE clone() {
      try {
        return (SAMPLE) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e); // should not occur.
      }
    }

    @SuppressWarnings("unchecked")
    final SAMPLE self() {
      return (SAMPLE) this;
    }

    final SAMPLE init(double g, int delta) {
      _g = g;
      _delta = delta;
      _count = 1;
      return self();
    }

    final SAMPLE merge0(@Nonnull SAMPLE other, boolean max, boolean min) {
      return merge(other, max, min);
    }

    @SuppressWarnings("unchecked")
    protected SAMPLE merge(@Nonnull SAMPLE other, boolean max, boolean min) {
      _g += other.g();
      _count += other.count();
      return self();
    }

    protected final double g() {
      return _g;
    }

    final int count() {
      return _count;
    }

    final int delta() {
      return _delta;
    }

    final double error() {
      return g() + delta();
    }

    static double error(@Nonnull AbstractSample cur, @Nonnull AbstractSample prev) {
      return cur.g() + prev.error();
    }
  }
}
