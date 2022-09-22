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
    private long _total429Retries;
    private long _totalRetriedKeys;
    private long _totalRetriesWinner;
    private long _totalRetriesDiscarded;
    private long _totalRetriesError;
    private long _totalDiscardedBytes;
    private long _totalRequestsSent;
    private long _totalRequestsReceived;
    private long _total503Retries;
    private long _total503RetriesWinner;
    private long _total503RetriesError;

    public Delta() {
    }

    Delta(Delta x, Delta y) {
      _totalRetries = x._totalRetries + y._totalRetries;
      _total429Retries = x._total429Retries + y._total429Retries;
      _totalRetriedKeys = x._totalRetriedKeys + y._totalRetriedKeys;
      _totalRetriesWinner = x._totalRetriesWinner + y._totalRetriesWinner;
      _totalRetriesDiscarded = x._totalRetriesDiscarded + y._totalRetriesDiscarded;
      _totalRetriesError = x._totalRetriesError + y._totalRetriesError;
      _totalDiscardedBytes = x._totalDiscardedBytes + y._totalDiscardedBytes;
      _totalRequestsSent = x._totalRequestsSent + y._totalRequestsSent;
      _totalRequestsReceived = x._totalRequestsReceived + y._totalRequestsReceived;
      _total503Retries = x._total503Retries + y._total503Retries;
      _total503RetriesWinner = x._total503RetriesWinner + y._total503RetriesWinner;
      _total503RetriesError = x._total503RetriesError + y._total503RetriesError;
    }

    void accumulate(Delta delta) {
      _totalRetries += delta._totalRetries;
      _total429Retries += delta._total429Retries;
      _totalRetriedKeys += delta._totalRetriedKeys;
      _totalRetriesWinner += delta._totalRetriesWinner;
      _totalRetriesDiscarded += delta._totalRetriesDiscarded;
      _totalRetriesError += delta._totalRetriesError;
      _totalDiscardedBytes += delta._totalDiscardedBytes;
      _totalRequestsSent += delta._totalRequestsSent;
      _totalRequestsReceived += delta._totalRequestsReceived;
      _total503Retries += delta._total503Retries;
      _total503RetriesWinner += delta._total503RetriesWinner;
      _total503RetriesError += delta._total503RetriesError;
    }

    public void apply() {
      _accumulator.accept(this);
      _totalRetries = 0;
      _total429Retries = 0;
      _totalRetriedKeys = 0;
      _totalRetriesWinner = 0;
      _totalRetriesDiscarded = 0;
      _totalRetriesError = 0;
      _totalDiscardedBytes = 0;
      _totalRequestsSent = 0;
      _totalRequestsReceived = 0;
      _total503Retries = 0;
      _total503RetriesWinner = 0;
      _total503RetriesError = 0;
    }

    public void incrementFanoutRequestsSent(int fanout) {
      _totalRequestsSent += fanout;
    }

    public void incrementTotalRequestsReceived() {
      _totalRequestsReceived++;
    }

    public void incrementTotalRetries() {
      _totalRetries++;
    }

    public void incrementTotalRetriesOn429() {
      _total429Retries++;
    }

    public void incrementTotalRetriedKeys(int retriedKeys) {
      _totalRetriedKeys += retriedKeys;
    }

    public void incrementTotalRetriesDiscarded(long bytes) {
      _totalRetriesDiscarded++;
      _totalDiscardedBytes += bytes;
    }

    public void incrementTotalRetriesWinner() {
      _totalRetriesWinner++;
    }

    public void incrementTotalRetriesError() {
      _totalRetriesError++;
    }

    public void incrementTotalRetriesOn503() {
      _total503Retries++;
    }

    public void incrementTotalRetriesOn503Winner() {
      _total503RetriesWinner++;
    }

    public void incrementTotalRetriesOn503Error() {
      _total503RetriesError++;
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
  public long getTotalRetriedOn429() {
    return data()._total429Retries;
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

  @Override
  public long getTotalDiscardedBytes() {
    return data()._totalDiscardedBytes;
  }

  @Override
  public long getTotalRequestsSent() {
    return data()._totalRequestsSent;
  }

  @Override
  public long getTotalRequestsReceived() {
    return data()._totalRequestsReceived;
  }

  @Override
  public long getTotalRetriesOn503() {
    return data()._total503Retries;
  }

  @Override
  public long getTotalRetriesOn503Winner() {
    return data()._total503RetriesWinner;
  }

  @Override
  public long getTotalRetriesOn503Error() {
    return data()._total503RetriesError;
  }
}
