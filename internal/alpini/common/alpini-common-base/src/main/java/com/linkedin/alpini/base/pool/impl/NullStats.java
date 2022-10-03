package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.statistics.LongStats;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class NullStats implements CallTracker.CallStats {
  private static final NullStats INSTANCE = new NullStats();

  public static CallTracker.CallStats getInstance() {
    return INSTANCE;
  }

  private NullStats() {
  }

  @Override
  public long getCallCountTotal() {
    return 0;
  }

  @Override
  public long getCallStartCountTotal() {
    return 0;
  }

  @Override
  public long getErrorCountTotal() {
    return 0;
  }

  @Override
  public int getConcurrency() {
    return 0;
  }

  @Override
  public double getAverageConcurrency1min() {
    return 0;
  }

  @Override
  public double getAverageConcurrency5min() {
    return 0;
  }

  @Override
  public double getAverageConcurrency15min() {
    return 0;
  }

  @Override
  public int getMaxConcurrency1min() {
    return 0;
  }

  @Override
  public int getMaxConcurrency5min() {
    return 0;
  }

  @Override
  public int getMaxConcurrency15min() {
    return 0;
  }

  @Override
  public int getStartFrequency1min() {
    return 0;
  }

  @Override
  public int getStartFrequency5min() {
    return 0;
  }

  @Override
  public int getStartFrequency15min() {
    return 0;
  }

  @Override
  public int getErrorFrequency1min() {
    return 0;
  }

  @Override
  public int getErrorFrequency5min() {
    return 0;
  }

  @Override
  public int getErrorFrequency15min() {
    return 0;
  }

  @Override
  public long getOutstandingStartTimeAvg() {
    return 0;
  }

  @Override
  public int getOutstandingCount() {
    return 0;
  }

  private final LongStats _longStats = new LongStats() {
    @Override
    public long getLongCount() {
      return 0;
    }

    @Override
    public double getAverage() {
      return 0;
    }

    @Override
    public double getStandardDeviation() {
      return Double.NaN;
    }

    @Override
    public Long getMinimum() {
      return null;
    }

    @Override
    public Long getMaximum() {
      return null;
    }

    @Override
    public Long get50Pct() {
      return null;
    }

    @Override
    public Long get90Pct() {
      return null;
    }

    @Override
    public Long get95Pct() {
      return null;
    }

    @Override
    public Long get99Pct() {
      return null;
    }

    @Override
    public Long get99_9Pct() {
      return null;
    }
  };

  @Override
  public LongStats getCallTimeStats() {
    return _longStats;
  }
}
