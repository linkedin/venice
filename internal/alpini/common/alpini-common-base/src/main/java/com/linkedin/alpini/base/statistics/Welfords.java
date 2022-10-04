package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.misc.Msg;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * Welford's algorithm for calculating variance.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 * @see "B.P.Welford (1962)"
 */
public abstract class Welfords {
  private static final AtomicReferenceFieldUpdater<Welfords, Result> STATE =
      AtomicReferenceFieldUpdater.newUpdater(Welfords.class, Result.class, "_state");

  private volatile Result _state;

  private static final ThreadLocal<LocalState> LOCAL = ThreadLocal.withInitial(LocalState::new);

  protected Welfords() {
    reset();
  }

  protected Welfords(Welfords previous) {
    _state = previous._state;
  }

  public final void reset() {
    _state = new Result(0, 0.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE);
  }

  public final void accept(double value) {
    if (Double.isFinite(value)) {
      LocalState local = LOCAL.get();
      local.value = value;
      STATE.updateAndGet(this, local.acceptFunction);
      local.done();
    }
  }

  public void merge(Welfords other) {
    Result result = other.getResult();
    if (result._count == 0) {
      return;
    }
    LocalState local = LOCAL.get();
    local.other = result;
    STATE.updateAndGet(this, local.mergeFunction);
    local.done();
  }

  public @Nonnull Result getResult() {
    LocalState local = LOCAL.get();
    STATE.updateAndGet(this, local.getFunction);
    return local.getLast();
  }

  public static class LongWelford extends Welfords implements LongConsumer {
    public static final Collector<Long, LongWelford, Result> COLLECTOR = Collector.of(
        LongWelford::new,
        LongWelford::accept,
        LongWelford::combine,
        Welfords::getResult,
        Collector.Characteristics.UNORDERED);

    public LongWelford() {
      super();
    }

    protected LongWelford(LongWelford longWelford) {
      super(longWelford);
    }

    @Override
    public final void accept(long value) {
      super.accept(value);
    }

    private LongWelford combine(LongWelford other) {
      LongWelford merged = new LongWelford(this);
      merged.merge(other);
      return merged;
    }
  }

  public static class DoubleWelford extends Welfords implements DoubleConsumer {
    public static final Collector<Double, DoubleWelford, Result> COLLECTOR = Collector.of(
        DoubleWelford::new,
        DoubleWelford::accept,
        DoubleWelford::combine,
        Welfords::getResult,
        Collector.Characteristics.UNORDERED);

    public DoubleWelford() {
      super();
    }

    protected DoubleWelford(DoubleWelford longWelford) {
      super(longWelford);
    }

    private DoubleWelford combine(DoubleWelford other) {
      DoubleWelford merged = new DoubleWelford(this);
      merged.merge(other);
      return merged;
    }
  }

  private static final class LocalState {
    private double value;
    private Result other;
    private Result result;
    private Result idle;
    private Result last;

    final UnaryOperator<Result> acceptFunction = this::accept;
    final UnaryOperator<Result> mergeFunction = this::merge;
    final UnaryOperator<Result> getFunction = this::get;

    private Result get(Result state) {
      return result(state).recycle(state._count, state._mean, state._m2, state._min, state._max);
    }

    private Result accept(Result state) {
      long count = state._count + 1;
      double mean = state._mean;
      double m2 = state._m2;
      double delta = value - mean;
      m2 += delta * (value - (mean += delta / count)); // SUPPRESS CHECKSTYLE InnerAssignment
      return result(state).recycle(count, mean, m2, Math.min(state._min, value), Math.max(state._max, value));
    }

    private Result merge(Result state) {
      if (state._count == 0) {
        if (idle == null) {
          idle = result;
        }
        return other;
      } else {
        double delta = other._mean - state._mean;
        double ma = state._m2 * (state._count - 1);
        double mb = other._m2 * (other._count - 1);
        long count = state._count + other._count;
        return result(last).recycle(
            count,
            ((state._mean * state._count) + (other._mean * other._count)) / count,
            (ma + mb + delta * delta * state._count * other._count) / count,
            Math.min(state._min, other._min),
            Math.max(state._max, other._max));
      }
    }

    private Result result(Result state) {
      last = state;
      if (result == null) {
        if (idle == null) {
          result = new Result();
        } else {
          result = idle;
          idle = null;
        }
      }
      return result;
    }

    private Result getLast() {
      result = null;
      Result last = this.last;
      this.last = null;
      return last;
    }

    private void done() {
      result = null;
      if (idle == null) {
        idle = last;
        last = null;
      }
    }
  }

  public static final class Result implements StringBuilderFormattable {
    private long _count;
    private double _mean;
    private double _m2;
    private double _min;
    private double _max;

    private Result() {
    }

    private Result(long count, double mean, double m2, double min, double max) {
      _count = count;
      _mean = mean;
      _m2 = m2;
      _min = min;
      _max = max;
    }

    private Result recycle(long count, double mean, double m2, double min, double max) {
      _count = count;
      _mean = mean;
      _m2 = m2;
      _min = min;
      _max = max;
      return this;
    }

    public long getCount() {
      return _count;
    }

    public double getAverage() {
      return _count > 0 ? _mean : Double.NaN;
    }

    private double variance() {
      return _m2 / (_count - 1);
    }

    public double getVariance() {
      return _count > 1 ? variance() : Double.NaN;
    }

    public double getStandardDeviation() {
      return _count > 1 ? Math.sqrt(variance()) : Double.NaN;
    }

    public double getMin() {
      return _min != Double.MAX_VALUE ? _min : 0.0;
    }

    public double getMax() {
      return _max != Double.MIN_VALUE ? _max : 0.0;
    }

    @Override
    public void formatTo(StringBuilder buffer) {
      buffer.append("count=")
          .append(getCount())
          .append(" average=")
          .append(formatDouble(getAverage()))
          .append(" stddev=")
          .append(formatDouble(getStandardDeviation()))
          .append(" min=")
          .append(formatDouble(getMin()))
          .append(" max=")
          .append(formatDouble(getMax()));
    }

    private long formatDouble(double value) {
      return Double.isNaN(value) ? 0 : (long) value;
    }

    @Override
    public String toString() {
      StringBuilder builder = Msg.stringBuilder();
      formatTo(builder);
      return builder.toString();
    }
  }
}
