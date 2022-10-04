package com.linkedin.alpini.base.statistics;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Extend the {@link LongQuantileEstimation} class and add computation for the mean and standard deviation of
 * the supplied values. The statistics are summarised in a {@link LongStats} object and the statistics are reset.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LongStatsAggregator extends LongQuantileEstimation {
  private static final Quantiles QUANTILES = new Quantiles(0.00, 0.50, 0.90, 0.95, 0.99, 0.999, 1.00);

  public LongStatsAggregator(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  @Override
  protected @Nonnull LongData newData() {
    return new LongData();
  }

  protected final class LongData extends Data {
    protected final Welfords.LongWelford _stddev = new Welfords.LongWelford();

    @Override
    public void accumulate(Sample v) {
      _stddev.accept(v._value);
      super.accumulate(v);
    }

    @Override
    public Data combine(Data other) {
      _stddev.merge(((LongData) other)._stddev);
      return super.combine(other);
    }
  }

  public LongStats getLongStats() {
    Welfords.Result[] stddev = new Welfords.Result[1];
    List<Sample> samples = queryAndReset(QUANTILES, d -> stddev[0] = ((LongData) d)._stddev.getResult());

    Long minimum;
    Long maximum;
    Long pct50;
    Long pct90;
    Long pct95;
    Long pct99;
    Long pct999;

    if (samples.isEmpty()) {
      minimum = null;
      maximum = null;
      pct50 = null;
      pct90 = null;
      pct95 = null;
      pct99 = null;
      pct999 = null;
    } else {
      Iterator<Sample> it = samples.iterator();
      minimum = it.next()._value;
      pct50 = it.next()._value;
      pct90 = it.next()._value;
      pct95 = it.next()._value;
      pct99 = it.next()._value;
      pct999 = it.next()._value;
      maximum = it.next()._value;
    }

    return new LongStatsImpl(stddev[0], minimum, maximum, pct50, pct90, pct95, pct99, pct999);
  }
}
