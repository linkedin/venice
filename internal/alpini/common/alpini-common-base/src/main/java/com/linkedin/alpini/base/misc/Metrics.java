/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import java.util.Arrays;
import javax.annotation.Nonnull;


public class Metrics {
  private static final int METRIC_COUNT = MetricNames.values().length;
  public static final long UNSET_VALUE = -1L;

  private final @Nonnull long[] metricValues;
  private transient Object _path;

  public Metrics() {
    this.metricValues = new long[METRIC_COUNT];
    Arrays.fill(this.metricValues, UNSET_VALUE);
  }

  public void setMetric(MetricNames name, long value) {
    this.metricValues[name.ordinal()] = value;
  }

  public long get(MetricNames name) {
    return this.metricValues[name.ordinal()];
  }

  public void setPath(Object path) {
    _path = path;
  }

  public <P> P getPath() {
    return (P) _path;
  }
}
