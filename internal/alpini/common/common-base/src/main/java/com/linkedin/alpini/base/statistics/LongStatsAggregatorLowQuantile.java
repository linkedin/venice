package com.linkedin.alpini.base.statistics;

import java.util.Iterator;
import java.util.List;


/**
 * Extend the {@link LongStatsAggregator} class to only track low quantile points.
 * The statistics are summarised in a {@link LongStatsLowQuantile} object and the statistics are reset.
 *
 * @author Guanlin Lu {@literal <gulu@linkedin.com>}
 */
public class LongStatsAggregatorLowQuantile extends LongStatsAggregator {
  private static final AbstractQuantileEstimation.Quantiles LOW_QUANTILES =
      new AbstractQuantileEstimation.Quantiles(0.00, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0);

  public LongStatsAggregatorLowQuantile(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  public LongStatsLowQuantile getLongStatsLowQuantile() {
    Welfords.Result[] stddev = new Welfords.Result[1];
    List<Sample> samples = queryAndReset(LOW_QUANTILES, d -> stddev[0] = ((LongData) d)._stddev.getResult());

    Long minimum;
    Long maximum;
    Long pct50;
    Long pct10;
    Long pct5;
    Long pct1;
    Long pct01;

    if (samples.isEmpty()) {
      minimum = null;
      maximum = null;
      pct01 = null;
      pct1 = null;
      pct5 = null;
      pct10 = null;
      pct50 = null;
    } else {
      Iterator<Sample> it = samples.iterator();
      minimum = it.next()._value;
      pct01 = it.next()._value;
      pct1 = it.next()._value;
      pct5 = it.next()._value;
      pct10 = it.next()._value;
      pct50 = it.next()._value;
      maximum = it.next()._value;
    }

    return new LongStatsLowQuantileImpl(stddev[0], minimum, maximum, pct01, pct1, pct5, pct10, pct50);
  }

}
