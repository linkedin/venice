package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.concurrency.ConcurrentAccumulator;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.statistics.AbstractQuantileEstimation;
import com.linkedin.alpini.base.statistics.LongStats;
import com.linkedin.alpini.base.statistics.LongStatsAggregator;
import com.linkedin.alpini.base.statistics.LongStatsArrayAggregator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * Tracker for "load average" of a call.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class CallTrackerImpl implements CallTracker {
  private static final int SECONDS_PER_BUCKET = 1;
  private static final long NANOS_PER_BUCKET = TimeUnit.SECONDS.toNanos(SECONDS_PER_BUCKET);
  private static final int NUMBER_OF_BUCKETS = 15 * 60 / SECONDS_PER_BUCKET + 1;
  private static final long SUNSET_NANOS = NANOS_PER_BUCKET * NUMBER_OF_BUCKETS;
  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

  private static final double LONG_STATS_EPSILON = 0.0005;
  private static final int LONG_STATS_SAMPLES = 2000;

  private static final Stats NULL_STATS = new Stats();

  private final ReentrantLock _lock = new ReentrantLock();

  private int _lastIndex;
  private long _nextNanos = Time.nanoTime();
  private final long[] _nanosBuckets = new long[NUMBER_OF_BUCKETS];
  private final long[] _startBuckets = new long[NUMBER_OF_BUCKETS];
  private final long[] _errorBuckets = new long[NUMBER_OF_BUCKETS];
  private final int[] _maxConBuckets = new int[NUMBER_OF_BUCKETS];

  public enum Mode {
    SKIP_LIST {
      @Override
      LongStatsAggregator constructor(double epsilon, int samples) {
        return new LongStatsAggregator(epsilon, samples);
      }
    },
    ARRAY_LIST {
      @Override
      LongStatsAggregator constructor(double epsilon, int samples) {
        return new LongStatsArrayAggregator(epsilon, samples);
      }
    };

    abstract LongStatsAggregator constructor(double epsilon, int samples);
  }

  public static Mode defaultMode = Mode.SKIP_LIST;

  private final LongStatsAggregator _callTimeStatsAggregator =
      defaultMode.constructor(LONG_STATS_EPSILON, LONG_STATS_SAMPLES);
  private final AtomicIntegerArray _concurrencyArray;
  private final AtomicIntegerArray _maxConcurrencyArray;

  private final ConcurrentAccumulator<Change, Stats, Stats> _stats;
  private long _lastResetTime = Time.currentTimeMillis();
  private long _lastStartTime = 0L;

  private Predicate<Throwable> _testSuccessful = ignored -> false;

  public CallTrackerImpl() {
    this(Runtime.getRuntime().availableProcessors());
  }

  public CallTrackerImpl(@Nonnegative int ncpu) {
    this(ncpu, ConcurrentAccumulator.defaultMode);
  }

  public CallTrackerImpl(@Nonnegative int ncpu, @Nonnull ConcurrentAccumulator.Mode accumulatorMode) {
    _concurrencyArray = new AtomicIntegerArray(cpuBuckets(Math.max(1, Math.min(ncpu, 64))));
    _maxConcurrencyArray = new AtomicIntegerArray(_concurrencyArray.length());
    _stats = new ConcurrentAccumulator<>(accumulatorMode, Stats.COLLECTOR);
  }

  private static final ThreadLocal<LocalState> LOCAL = ThreadLocal.withInitial(LocalState::new);

  private static final class LocalState {
    private final Reset reset = new Reset();
    private final Start start = new Start();
    private final EndTimeSuccess endSuccess = new EndTimeSuccess();
    private final EndTimeFailure endFailure = new EndTimeFailure();
    private State last;
    private State result;
    private State idle;

    private Start start(long startTimeNanos) {
      start._startTimeNanos = startTimeNanos;
      return start;
    }

    private EndTimeSuccess endSuccess(long now, long startTimeNanos) {
      endSuccess.now = now;
      endSuccess.startTimeNanos = startTimeNanos;
      return endSuccess;
    }

    private EndTimeFailure endFailure(long now, long startTimeNanos) {
      endFailure.now = now;
      endFailure.startTimeNanos = startTimeNanos;
      return endFailure;
    }

    private Stats combine(Stats stats, Stats other) {
      return combine(state(stats), state(other));
    }

    private Stats combine(State state, State other) {
      state._started += other._started;
      state._completed += other._completed;
      state._completedWithError += other._completedWithError;
      state._nanosRunning += other._nanosRunning;
      state._startTimeSum += other._startTimeSum;
      state._concurrency += other._concurrency;
      idle = other;
      return new Stats(state);
    }

    void localDone() {
      result = null;
      if (idle == null) {
        idle = last;
        last = null;
      }
    }

    public State state(Stats stats) {
      State oldState = stats.get();
      for (;;) {
        State newState = copyStats(oldState);
        if (stats.compareAndSet(oldState, newState)) {
          break;
        }
        idle = newState;
        oldState = stats.get();
      }
      State last = this.last;
      this.last = null;
      localDone();
      return last;
    }

    private State copyStats(State state) {
      return result(state).recycle(
          state._started,
          state._completed,
          state._completedWithError,
          state._nanosRunning,
          state._startTimeSum,
          state._concurrency);
    }

    private State result(State state) {
      last = state;
      if (result == null) {
        if (idle == null) {
          result = new State();
        } else {
          result = idle;
          idle = null;
        }
      }
      return result;
    }

    private final class Reset extends Change {
      @Override
      public State apply(State state) {
        long adjust = Math.min(state._started, state._completed);
        return result(state).recycle(
            state._started - adjust,
            state._completed - adjust,
            0,
            state._nanosRunning,
            state._startTimeSum,
            state._concurrency);
      }

      public void done() {
        localDone();
      }
    }

    private final class Start extends Change {
      private long _startTimeNanos;

      @Override
      public State apply(State state) {
        return result(state).recycle(
            state._started + 1,
            state._completed,
            state._completedWithError,
            state._nanosRunning - _startTimeNanos,
            state._startTimeSum + _startTimeNanos,
            state._concurrency + 1);
      }

      public void done() {
        localDone();
      }
    }

    private final class EndTimeSuccess extends Change {
      private long now;
      private long startTimeNanos;

      @Override
      public State apply(State state) {
        return result(state).recycle(
            state._started,
            state._completed + 1,
            state._completedWithError,
            state._nanosRunning + now,
            state._startTimeSum - startTimeNanos,
            state._concurrency - 1);
      }

      public void done() {
        localDone();
      }
    }

    private final class EndTimeFailure extends Change {
      private long now;
      private long startTimeNanos;

      @Override
      public State apply(State state) {
        return result(state).recycle(
            state._started,
            state._completed + 1,
            state._completedWithError + 1,
            state._nanosRunning + now,
            state._startTimeSum - startTimeNanos,
            state._concurrency - 1);
      }

      public void done() {
        localDone();
      }
    }
  }

  private static abstract class Change implements UnaryOperator<State> {
    public abstract State apply(State state);

    public abstract void done();
  }

  private static final class State {
    long _started;
    long _completed;
    long _completedWithError;
    long _nanosRunning;
    long _startTimeSum;
    long _concurrency;

    private State recycle(
        long started,
        long completed,
        long completedWithError,
        long nanosRunning,
        long startTimeSum,
        long concurrency) {
      _started = started;
      _completed = completed;
      _completedWithError = completedWithError;
      _nanosRunning = nanosRunning;
      _startTimeSum = startTimeSum;
      _concurrency = concurrency;
      return this;
    }

    private int concurrency() {
      return Math.toIntExact(_concurrency);
    }
  }

  private static final class Stats extends AtomicReference<State> {
    Stats() {
      this(new State());
    }

    Stats(State state) {
      super(state);
    }

    private static final Collector<Change, Stats, Stats> COLLECTOR = Collector.of(
        Stats::new,
        Stats::accumulate,
        Stats::combine,
        Function.identity(),
        Collector.Characteristics.UNORDERED,
        Collector.Characteristics.IDENTITY_FINISH);

    private Stats combine(Stats other) {
      return LOCAL.get().combine(this, other);
    }

    private void accumulate(Change change) {
      super.updateAndGet(change);
      change.done();
    }

    private State state() {
      return LOCAL.get().state(this);
    }
  }

  private void tick(final long now) {
    assert _lock.isHeldByCurrentThread();
    if (_nextNanos + SUNSET_NANOS < now) {
      _nextNanos = now - SUNSET_NANOS;
      _lastIndex = 0;
    }
    long nextNanos = _nextNanos;
    if (nextNanos <= now) {
      Stats stats = stats();
      int maxConcurrency = 0;
      for (int i = _maxConcurrencyArray.length() - 1; i >= 0; i--) {
        maxConcurrency += _maxConcurrencyArray.getAndSet(i, 0);
      }
      int index = _lastIndex;
      do {
        if (NUMBER_OF_BUCKETS == ++index) {
          index = 0;
        }
        State state = stats.state();
        _nanosBuckets[index] = state._nanosRunning + state._concurrency * nextNanos;
        _startBuckets[index] = state._started;
        _errorBuckets[index] = state._completedWithError;
        _maxConBuckets[index] = maxConcurrency;
        nextNanos += NANOS_PER_BUCKET;
      } while (nextNanos <= now);
      _lastIndex = index;
      _nextNanos = nextNanos;
    }
  }

  public void setTestSuccessful(@Nonnull Predicate<Throwable> test) {
    _testSuccessful = test;
  }

  protected boolean isSuccessfulException(@Nonnull Throwable exception) {
    return _testSuccessful.test(exception);
  }

  private boolean isSuccessful(Throwable exception) {
    return exception == null || exception != GENERIC_EXCEPTION && isSuccessfulException(exception);
  }

  private final class Completion extends AtomicReference<Completion> implements CallCompletion {
    private final long _startTimeNanos;
    private final long _threadId = Thread.currentThread().getId();
    private final int _slot = foldUp(_threadId) & (_concurrencyArray.length() - 1);

    private Completion(long startTimeNanos) {
      this._startTimeNanos = startTimeNanos;
      set(this);
    }

    @Override
    public void close(@Nonnegative long now) {
      long duration = Math.max(0L, now - _startTimeNanos);
      LocalState local = LOCAL.get();
      close(local, now, duration, local.endSuccess(now, _startTimeNanos));
    }

    @Override
    public void closeWithError(@Nonnegative long endTimeNanos) {
      closeWithError(endTimeNanos, GENERIC_EXCEPTION);
    }

    @Override
    public void closeWithError(@Nonnegative long now, @Nonnull Throwable error) {
      long duration = Math.max(0L, now - _startTimeNanos);
      LocalState local = LOCAL.get();
      close(
          local,
          now,
          duration,
          isSuccessful(error) ? local.endSuccess(now, _startTimeNanos) : local.endFailure(now, _startTimeNanos));
    }

    private void close(LocalState local, long now, long duration, Change change) {
      if (!compareAndSet(this, null)) {
        return;
      }
      _stats.accept(change);
      _callTimeStatsAggregator.accept(duration);
      _concurrencyArray.getAndDecrement(_slot);
      checkTick(now);
    }
  }

  @Override
  @Nonnull
  public CallCompletion startCall(@Nonnegative long startTimeNanos) {
    _lastStartTime = Math.max(_lastStartTime, startTimeNanos);

    Completion cc = new Completion(startTimeNanos);

    checkTick(startTimeNanos);

    LocalState local = LOCAL.get();
    _stats.accept(local.start(startTimeNanos));

    int con = _concurrencyArray.incrementAndGet(cc._slot);
    if (con > _maxConcurrencyArray.get(cc._slot)) {
      _maxConcurrencyArray.accumulateAndGet(cc._slot, con, Math::max);
    }

    return cc;
  }

  @Override
  public void trackCallWithError(long duration, @Nonnull TimeUnit timeUnit, Throwable throwable) {
    long now = Time.nanoTime();
    duration = timeUnit.toNanos(duration);
    long startTimeNanos = now - duration;

    _lastStartTime = Math.max(_lastStartTime, startTimeNanos);
    LocalState local = LOCAL.get();
    _stats.accept(local.start(startTimeNanos));

    _stats.accept(
        isSuccessful(throwable) ? local.endSuccess(now, startTimeNanos) : local.endFailure(now, startTimeNanos));

    _callTimeStatsAggregator.accept(duration);

    checkTick(now);
  }

  private void checkTick(long now) {
    if (_nextNanos <= now && _lock.tryLock()) {
      try {
        tick(now);
      } finally {
        _lock.unlock();
      }
    }
  }

  @Nonnull
  private Stats stats() {
    Stats stats = _stats.get();
    if (stats == null) {
      stats = NULL_STATS;
    }
    return stats;
  }

  private long getTotalRuntimeNanos(State state, long now) {
    return state._nanosRunning + state._concurrency * now;
  }

  public long getTotalRuntimeNanos() {
    return getTotalRuntimeNanos(stats().state(), Time.nanoTime());
  }

  @Override
  public long getCurrentStartCountTotal() {
    return stats().state()._started;
  }

  @Override
  public long getCurrentCallCountTotal() {
    return stats().state()._completed;
  }

  @Override
  public long getCurrentErrorCountTotal() {
    return stats().state()._completedWithError;
  }

  @Override
  public int getCurrentConcurrency() {
    return stats().state().concurrency();
  }

  private static int calcIndex(int pos, int buckets) {
    pos -= buckets - 1;
    if (pos < 0) {
      pos += NUMBER_OF_BUCKETS;
    }
    return pos;
  }

  /**
   * Returns the average concurrency over a period of time.
   * @return array of 1, 5 and 15 minute average concurrency.
   */
  @Override
  @Nonnull
  public double[] getAverageConcurrency() {
    long now = Time.nanoTime();
    _lock.lock();
    try {
      tick(now);
      return getAverageConcurrency(now, stats().state());
    } finally {
      _lock.unlock();
    }
  }

  private double[] getAverageConcurrency(long now, State state) {
    assert _lock.isHeldByCurrentThread();
    long delta1;
    long delta5;
    long delta15;

    long currentTotal = getTotalRuntimeNanos(state, now);

    int lastIndex = _lastIndex;

    int index1 = calcIndex(lastIndex, 60 / SECONDS_PER_BUCKET);
    int index5 = calcIndex(lastIndex, 300 / SECONDS_PER_BUCKET);
    int index15 = calcIndex(lastIndex, 900 / SECONDS_PER_BUCKET);

    delta1 = currentTotal - _nanosBuckets[index1];
    delta5 = currentTotal - _nanosBuckets[index5];
    delta15 = currentTotal - _nanosBuckets[index15];

    return new double[] { ((double) delta1) / (60 * NANOS_PER_SECOND), ((double) delta5) / (300 * NANOS_PER_SECOND),
        ((double) delta15) / (900 * NANOS_PER_SECOND) };
  }

  @Override
  @Nonnull
  public int[] getMaxConcurrency() {
    checkTick(Time.nanoTime());
    return getMaxConcurrency(stats().state());
  }

  private int[] getMaxConcurrency(State state) {
    int lastIndex = _lastIndex;

    int index1 = calcIndex(lastIndex, 60 / SECONDS_PER_BUCKET);
    int index5 = calcIndex(lastIndex, 300 / SECONDS_PER_BUCKET);
    int index15 = calcIndex(lastIndex, 900 / SECONDS_PER_BUCKET);

    int index = lastIndex;

    int concurrency1 = 0;
    for (; index != index1; index = (index == 0 ? NUMBER_OF_BUCKETS : index) - 1) {
      concurrency1 = Math.max(concurrency1, _maxConBuckets[index]);
    }

    int concurrency5 = concurrency1;
    for (; index != index5; index = (index == 0 ? NUMBER_OF_BUCKETS : index) - 1) {
      concurrency5 = Math.max(concurrency5, _maxConBuckets[index]);
    }

    int concurrency15 = concurrency5;
    for (; index != index15; index = (index == 0 ? NUMBER_OF_BUCKETS : index) - 1) {
      concurrency15 = Math.max(concurrency15, _maxConBuckets[index]);
    }

    return new int[] { concurrency1, concurrency5, concurrency15 };
  }

  private static int div(long numerator, int denominator) {
    return (int) ((numerator + denominator - 1) / denominator);
  }

  @Override
  @Nonnull
  public int[] getStartFrequency() {
    checkTick(Time.nanoTime());
    return getStartFrequency(stats().state());
  }

  @Override
  @Nonnull
  public long[] getStartCount() {
    checkTick(Time.nanoTime());
    return getStartCount(stats().state());
  }

  private int[] getStartFrequency(State state) {
    return getFrequency(state._started, _startBuckets);
  }

  private long[] getStartCount(State state) {
    return getDiffCount(state._started, _startBuckets);
  }

  @Override
  @Nonnull
  public int[] getErrorFrequency() {
    checkTick(Time.nanoTime());
    return getErrorFrequency(stats().state());
  }

  public long[] getErrorCount() {
    checkTick(Time.nanoTime());
    return getErrorCount(stats().state());
  }

  private int[] getErrorFrequency(State state) {
    return getFrequency(state._completedWithError, _errorBuckets);
  }

  private long[] getErrorCount(State state) {
    return getDiffCount(state._completedWithError, _errorBuckets);
  }

  private int[] getFrequency(long currentTotal, long[] buckets) {
    return getFrequency0(currentTotal, buckets);
  }

  private long[] getDiffCount(long currentTotal, long[] buckets) {
    int lastIndex = _lastIndex;
    int index1 = calcIndex(lastIndex, 60 / SECONDS_PER_BUCKET);
    int index5 = calcIndex(lastIndex, 300 / SECONDS_PER_BUCKET);
    int index15 = calcIndex(lastIndex, 900 / SECONDS_PER_BUCKET);
    long value1 = currentTotal - buckets[index1];
    long value5 = currentTotal - buckets[index5];
    long value15 = currentTotal - buckets[index15];
    return new long[] { value1, value5, value15 };
  }

  private int[] getFrequency0(long currentTotal, long[] buckets) {
    long[] diffCounts = getDiffCount(currentTotal, buckets);
    int value1 = div(diffCounts[0], 60);
    int value5 = div(diffCounts[1], 300);
    int value15 = div(diffCounts[2], 900);
    return new int[] { value1, value5, value15 };
  }

  @Override
  @Nonnull
  public CallStats getCallStats() {
    long now = Time.nanoTime();
    _lock.lock();
    try {
      tick(now);
      return new CallStatsImpl(now, this, stats().state(), _callTimeStatsAggregator.getLongStats());
    } finally {
      _lock.unlock();
    }
  }

  public AbstractQuantileEstimation.Quantile computeQuantile(long sample, @Nonnull TimeUnit timeUnit) {
    return _callTimeStatsAggregator.computeQuantile(timeUnit.toNanos(sample));
  }

  @Override
  public void reset() {
    _lastResetTime = Time.currentTimeMillis();
    LocalState local = LOCAL.get();
    _stats.accept(local.reset);
    _callTimeStatsAggregator.reset();
  }

  @Override
  public long getLastResetTime() {
    return _lastResetTime;
  }

  @Override
  public long getTimeSinceLastStartCall() {
    return _lastStartTime == 0L ? 0L : TimeUnit.NANOSECONDS.toMillis(Time.nanoTime() - _lastStartTime);
  }

  private static int cpuBuckets(int proc) {
    proc--;
    proc |= proc >>> 1;
    proc |= proc >>> 2;
    proc |= proc >>> 4;
    proc |= proc >>> 8;
    proc |= proc >>> 16;
    return Math.max(1, ++proc);
  }

  private static int foldUp(long value) {
    return (int) (0xffL & (value ^ (value >>> 8) ^ (value >>> 16) ^ (value >>> 24) ^ (value >>> 32) ^ (value >>> 40)
        ^ (value >>> 48) ^ (value >>> 52)));
  }

  private static final class CallStatsImpl implements CallStats {
    private final long _callCountTotal;
    private final long _callStartCountTotal;
    private final long _errorCountTotal;
    private final int _currentConcurrency;
    private final double[] _concurrencyAvg;
    private final int[] _maxConcurrency;
    private final int[] _errorFrequency;
    private final int[] _startFrequency;
    private final long _outstandingStartTimes;
    private final LongStats _callTimeStats;

    private CallStatsImpl(long now, CallTrackerImpl callTracker, State state, LongStats callTimeStats) {
      _callCountTotal = state._completed;
      _callStartCountTotal = state._started;
      _errorCountTotal = state._completedWithError;
      _currentConcurrency = state.concurrency();
      _concurrencyAvg = callTracker.getAverageConcurrency(now, state);
      _maxConcurrency = callTracker.getMaxConcurrency(state);
      _errorFrequency = callTracker.getErrorFrequency(state);
      _startFrequency = callTracker.getStartFrequency(state);
      _outstandingStartTimes = state._startTimeSum;
      _callTimeStats = callTimeStats;
    }

    @Override
    public long getCallCountTotal() {
      return _callCountTotal;
    }

    @Override
    public long getCallStartCountTotal() {
      return _callStartCountTotal;
    }

    @Override
    public long getErrorCountTotal() {
      return _errorCountTotal;
    }

    @Override
    public int getConcurrency() {
      return _currentConcurrency;
    }

    @Override
    public double getAverageConcurrency1min() {
      return _concurrencyAvg[0];
    }

    @Override
    public double getAverageConcurrency5min() {
      return _concurrencyAvg[1];
    }

    @Override
    public double getAverageConcurrency15min() {
      return _concurrencyAvg[2];
    }

    @Override
    public long getOutstandingStartTimeAvg() {
      int outstanding = getConcurrency();
      return outstanding > 0 ? _outstandingStartTimes / outstanding : 0;
    }

    @Override
    public int getOutstandingCount() {
      return getConcurrency();
    }

    @Override
    public int getMaxConcurrency1min() {
      return _maxConcurrency[0];
    }

    @Override
    public int getMaxConcurrency5min() {
      return _maxConcurrency[1];
    }

    @Override
    public int getMaxConcurrency15min() {
      return _maxConcurrency[2];
    }

    @Override
    public int getErrorFrequency1min() {
      return _errorFrequency[0];
    }

    @Override
    public int getErrorFrequency5min() {
      return _errorFrequency[1];
    }

    @Override
    public int getErrorFrequency15min() {
      return _errorFrequency[2];
    }

    @Override
    public int getStartFrequency1min() {
      return _startFrequency[0];
    }

    @Override
    public int getStartFrequency5min() {
      return _startFrequency[1];
    }

    @Override
    public int getStartFrequency15min() {
      return _startFrequency[2];
    }

    @Override
    public LongStats getCallTimeStats() {
      return _callTimeStats;
    }

    @Override
    public String toString() {
      return String.format(
          "callCountTotal=%d startCountTotal=%d errorCountTotal=%d"
              + " concurrency=%d concurrencyAvg=%.3f,%.3f,%.3f concurrencyMax=%d,%d,%d"
              + " startFrequency=%d,%d,%d errorFrequency=%d,%d,%d" + " outstanding=%d outstandingStartTimeAvg=%d %s",
          getCallCountTotal(),
          getCallStartCountTotal(),
          getErrorCountTotal(),
          getConcurrency(),
          getAverageConcurrency1min(),
          getAverageConcurrency5min(),
          getAverageConcurrency15min(),
          getMaxConcurrency1min(),
          getMaxConcurrency5min(),
          getMaxConcurrency15min(),
          getStartFrequency1min(),
          getStartFrequency5min(),
          getStartFrequency15min(),
          getErrorFrequency1min(),
          getErrorFrequency5min(),
          getErrorFrequency15min(),
          getOutstandingCount(),
          getOutstandingStartTimeAvg(),
          _callTimeStats);
    }
  }
}
