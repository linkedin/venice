/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class Metrics {
  private @Nonnull Map<MetricNames, TimeValue> _metrics = new EnumMap(MetricNames.class);
  private transient Object _path;

  public Map<MetricNames, TimeValue> getMetrics() {
    return _metrics;
  }

  public void setMetric(MetricNames name, TimeValue value) {
    _metrics.put(name, value);
  }

  public void setPath(Object path) {
    _path = path;
  }

  public <P> P getPath() {
    return (P) _path;
  }
}
