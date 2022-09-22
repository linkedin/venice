package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.misc.Msg;
import java.util.Formatter;
import org.apache.logging.log4j.util.StringBuilderFormattable;


public class LongStatsLowQuantileImpl implements LongStatsLowQuantile, StringBuilderFormattable {
  private final Welfords.Result _stddev;
  private final Long _minimum;
  private final Long _maximum;
  private final Long _pct01;
  private final Long _pct1;
  private final Long _pct5;
  private final Long _pct10;
  private final Long _pct50; // SUPPRESS CHECKSTYLE MemberName

  LongStatsLowQuantileImpl(
      Welfords.Result stddev,
      Long minimum,
      Long maximum,
      Long pct01,
      Long pct1,
      Long pct5,
      Long pct10,
      Long pct50) { // SUPPRESS CHECKSTYLE ParameterName
    _stddev = stddev;
    _minimum = minimum;
    _maximum = maximum;
    _pct01 = pct01;
    _pct1 = pct1;
    _pct5 = pct5;
    _pct10 = pct10;
    _pct50 = pct50;
  }

  @Override
  public long getLongCount() {
    return _stddev != null ? _stddev.getCount() : 0L;
  }

  @Override
  public double getAverage() {
    return _stddev != null ? _stddev.getAverage() : 0.0;
  }

  @Override
  public double getStandardDeviation() {
    return _stddev != null ? _stddev.getStandardDeviation() : 0.0;
  }

  @Override
  public Long getMinimum() {
    return _minimum;
  }

  @Override
  public Long getMaximum() {
    return _maximum;
  }

  @Override
  public Long get50Pct() {
    return _pct50;
  }

  @Override
  public Long get01Pct() {
    return _pct01;
  }

  @Override
  public Long get1Pct() {
    return _pct1;
  }

  @Override
  public Long get5Pct() {
    return _pct5;
  }

  @Override
  public Long get10Pct() { // SUPPRESS CHECKSTYLE MethodName
    return _pct10;
  }

  @Override
  public void formatTo(StringBuilder buffer) {
    new Formatter(buffer).format(
        "count=%d average=%.1f stddev=%.1f minimum=%d maximum=%d" + " pct0.1=%d pct1=%d pct5=%d pct10=%d pct50=%d",
        getCount(),
        getAverage(),
        getStandardDeviation(),
        getMinimum(),
        getMaximum(),
        get01Pct(),
        get1Pct(),
        get5Pct(),
        get10Pct(),
        get50Pct());
  }

  @Override
  public String toString() {
    StringBuilder builder = Msg.stringBuilder();
    formatTo(builder);
    return builder.toString();
  }
}
