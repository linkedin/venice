package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.statistics.LongStats;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/30/17.
 */
public final class NullCallTracker implements CallTracker {
  public static final CallTracker INSTANCE = new NullCallTracker();

  @Nonnull
  @Override
  public CallCompletion startCall(@Nonnegative long startTimeNanos) {
    return CALL_COMPLETION;
  }

  @Override
  public void trackCall(long duration, @Nonnull TimeUnit timeUnit) {
  }

  @Override
  public void trackCallWithError(long duration, @Nonnull TimeUnit timeUnit, Throwable throwable) {
  }

  @Override
  public long getCurrentStartCountTotal() {
    return 0;
  }

  @Override
  public long getCurrentCallCountTotal() {
    return 0;
  }

  @Override
  public long getCurrentErrorCountTotal() {
    return 0;
  }

  @Override
  public int getCurrentConcurrency() {
    return 0;
  }

  @Nonnull
  @Override
  public double[] getAverageConcurrency() {
    return new double[0];
  }

  @Nonnull
  @Override
  public int[] getMaxConcurrency() {
    return new int[0];
  }

  @Nonnull
  @Override
  public int[] getStartFrequency() {
    return new int[0];
  }

  @Nonnull
  @Override
  public long[] getStartCount() {
    return new long[0];
  }

  @Nonnull
  @Override
  public int[] getErrorFrequency() {
    return new int[0];
  }

  @Nonnull
  @Override
  public long[] getErrorCount() {
    return new long[0];
  }

  @Nonnull
  @Override
  public CallStats getCallStats() {
    return CALL_STATS;
  }

  @Override
  public void reset() {
  }

  @Override
  public long getLastResetTime() {
    return 0;
  }

  @Override
  public long getTimeSinceLastStartCall() {
    return 0;
  }

  private static final CallCompletion CALL_COMPLETION = new CallCompletion() {
    @Override
    public void close(@Nonnegative long endTimeNanos) {
    }

    @Override
    public void closeWithError(@Nonnegative long endTimeNanos, @Nonnull Throwable error) {
    }
  };

  private static final CallStats CALL_STATS = new CallStats() {
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

    @Override
    public LongStats getCallTimeStats() {
      return LONG_STATS;
    }
  };

  private static final LongStats LONG_STATS = new LongStats() {
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
      return 0;
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
}
