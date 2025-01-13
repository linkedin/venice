package com.linkedin.alpini.router.monitoring;

import com.linkedin.alpini.base.concurrency.ConcurrentAccumulator;
import java.util.function.Function;
import java.util.stream.Collector;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ScatterGatherStats implements ScatterGatherStatsMBean {
  private final ConcurrentAccumulator<Delta, Delta, Delta> _accumulator;

  public ScatterGatherStats() {
    _accumulator = new ConcurrentAccumulator<>(
        Collector.of(
            Delta::new,
            Delta::accumulate,
            Delta::new,
            Function.identity(),
            Collector.Characteristics.UNORDERED,
            Collector.Characteristics.IDENTITY_FINISH));
  }

  public class Delta {
    private long _totalRetries;
    private long _totalRetriedKeys;
    private long _totalRetriesWinner;
    private long _totalRetriesDiscarded;
    private long _totalRetriesError;

    public Delta() {
    }

    Delta(Delta x, Delta y) {
      _totalRetries = x._totalRetries + y._totalRetries;
      _totalRetriedKeys = x._totalRetriedKeys + y._totalRetriedKeys;
      _totalRetriesWinner = x._totalRetriesWinner + y._totalRetriesWinner;
      _totalRetriesDiscarded = x._totalRetriesDiscarded + y._totalRetriesDiscarded;
      _totalRetriesError = x._totalRetriesError + y._totalRetriesError;
    }

    void accumulate(Delta delta) {
      _totalRetries += delta._totalRetries;
      _totalRetriedKeys += delta._totalRetriedKeys;
      _totalRetriesWinner += delta._totalRetriesWinner;
      _totalRetriesDiscarded += delta._totalRetriesDiscarded;
      _totalRetriesError += delta._totalRetriesError;
    }

    public void apply() {
      _accumulator.accept(this);
      _totalRetries = 0;
      _totalRetriedKeys = 0;
      _totalRetriesWinner = 0;
      _totalRetriesDiscarded = 0;
      _totalRetriesError = 0;
    }

    public void incrementTotalRetries() {
      _totalRetries++;
    }

    public void incrementTotalRetriedKeys(int retriedKeys) {
      _totalRetriedKeys += retriedKeys;
    }

    public void incrementTotalRetriesDiscarded() {
      _totalRetriesDiscarded++;
    }

    public void incrementTotalRetriesWinner() {
      _totalRetriesWinner++;
    }

    public void incrementTotalRetriesError() {
      _totalRetriesError++;
    }
  }

  @Nonnull
  private Delta data() {
    Delta data = _accumulator.get();
    return data != null ? data : new Delta();
  }

  @Override
  public long getTotalRetries() {
    return data()._totalRetries;
  }

  @Override
  public long getTotalRetriedKeys() {
    return data()._totalRetriedKeys;
  }

  @Override
  public long getTotalRetriesDiscarded() {
    return data()._totalRetriesDiscarded;
  }

  @Override
  public long getTotalRetriesWinner() {
    return data()._totalRetriesWinner;
  }

  @Override
  public long getTotalRetriesError() {
    return data()._totalRetriesError;
  }
}
