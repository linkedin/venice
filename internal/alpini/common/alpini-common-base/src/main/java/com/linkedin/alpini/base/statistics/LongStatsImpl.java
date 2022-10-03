package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.misc.Msg;
import java.util.Formatter;
import org.apache.logging.log4j.util.StringBuilderFormattable;


final class LongStatsImpl implements LongStats, StringBuilderFormattable {
  private final Welfords.Result _stddev;
  private final Long _minimum;
  private final Long _maximum;
  private final Long _pct50;
  private final Long _pct90;
  private final Long _pct95;
  private final Long _pct99;
  private final Long _pct99_9; // SUPPRESS CHECKSTYLE MemberName

  LongStatsImpl(
      Welfords.Result stddev,
      Long minimum,
      Long maximum,
      Long pct50,
      Long pct90,
      Long pct95,
      Long pct99,
      Long pct99_9) { // SUPPRESS CHECKSTYLE ParameterName
    _stddev = stddev;
    _minimum = minimum;
    _maximum = maximum;
    _pct50 = pct50;
    _pct90 = pct90;
    _pct95 = pct95;
    _pct99 = pct99;
    _pct99_9 = pct99_9;
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
  public Long get90Pct() {
    return _pct90;
  }

  @Override
  public Long get95Pct() {
    return _pct95;
  }

  @Override
  public Long get99Pct() {
    return _pct99;
  }

  @Override
  public Long get99_9Pct() { // SUPPRESS CHECKSTYLE MethodName
    return _pct99_9;
  }

  @Override
  public void formatTo(StringBuilder buffer) {
    new Formatter(buffer).format(
        "count=%d average=%.1f stddev=%.1f minimum=%d maximum=%d" + " pct50=%d pct90=%d pct95=%d pct99=%d pct99.9=%d",
        getCount(),
        getAverage(),
        getStandardDeviation(),
        getMinimum(),
        getMaximum(),
        get50Pct(),
        get90Pct(),
        get95Pct(),
        get99Pct(),
        get99_9Pct());
  }

  @Override
  public String toString() {
    StringBuilder builder = Msg.stringBuilder();
    formatTo(builder);
    return builder.toString();
  }
}
