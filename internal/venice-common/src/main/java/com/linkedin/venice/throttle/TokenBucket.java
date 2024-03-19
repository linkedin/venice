package com.linkedin.venice.throttle;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @see <a href="http://en.wikipedia.org/wiki/Token_bucket">Token Bucket on Wikipedia</a>
 * This implementation aims to be very high performance with the goal of supporting a very large number of
 * TokenBuckets in an application; thus avoiding an auxilliary thread to refill the bucket.
 */
public class TokenBucket {
  private final long capacity;
  private final long refillAmount;
  private final long refillIntervalMs;
  private final float refillPerSecond;// only used for logging
  private final Clock clock;
  private final AtomicLong tokens;
  private final AtomicLong tokensRequestedSinceLastRefill;
  private volatile long previousRefillTime;
  private volatile long nextUpdateTime;

  /**
   * This constructor should only be used by tests.  Application should not specify it's own instance of Clock
   * @param capacity
   * @param refillAmount
   * @param refillInterval
   * @param refillUnit
   * @param clock
   */
  public TokenBucket(long capacity, long refillAmount, long refillInterval, TimeUnit refillUnit, Clock clock) {

    if (capacity <= 0) {
      throw new IllegalArgumentException("TokenBucket capacity " + capacity + " is not valid.  Must be greater than 0");
    }
    this.capacity = capacity;

    if (refillAmount <= 0) {
      throw new IllegalArgumentException(
          "TokenBucket refillAmount " + refillAmount + " is not valid.  Must be greater than 0");
    }
    this.refillAmount = refillAmount;

    if (refillInterval <= 0) {
      throw new IllegalArgumentException(
          "TokenBucket refillInterval " + refillInterval + " is not valid.  Must be greater than 0");
    }
    this.refillIntervalMs = refillUnit.toMillis(refillInterval);
    this.clock = clock;

    tokens = new AtomicLong(capacity);
    tokensRequestedSinceLastRefill = new AtomicLong(0);
    previousRefillTime = clock.millis();
    nextUpdateTime = previousRefillTime + refillIntervalMs;

    refillPerSecond = refillAmount / (float) refillUnit.toSeconds(refillInterval);
  }

  /**
   *
   * @param capacity The maximum number of tokens that the bucket can have at any one time.  Any refill beyond the
   *                 capacity is lost.  A capacity larger than the refillAmount supports bursting.
   * @param refillAmount The number of tokens added to the bucket each interval
   * @param refillInterval The interval of time between refills of the bucket
   * @param refillUnit The TimeUnit for the refillInterval
   */
  public TokenBucket(long capacity, long refillAmount, long refillInterval, TimeUnit refillUnit) {
    this(capacity, refillAmount, refillInterval, refillUnit, Clock.systemUTC());
  }

  /**
   * Check and add tokens if conditions are met. Note that token may have been updated by another thread even when the
   * function is short-circuited. Consumers of the token bucket should always retry.
   */
  private void update() {
    if (clock.millis() > nextUpdateTime) {
      synchronized (this) {
        long timeNow = clock.millis();
        if (timeNow > nextUpdateTime) {
          long refillCount = (timeNow - nextUpdateTime) / refillIntervalMs + 1;
          long totalRefillAmount = refillCount * refillAmount;
          tokens.getAndAccumulate(totalRefillAmount, (existing, toAdd) -> {
            long newTokens = existing + toAdd;
            if (newTokens > capacity) {
              return capacity;
            } else {
              return newTokens;
            }
          });
          previousRefillTime = timeNow;
          tokensRequestedSinceLastRefill.set(0);
          nextUpdateTime = timeNow + refillIntervalMs;
        }
      }
    }
  }

  /**
   * This method does not call #update(), so it is only accurate as of the last time #tryConsume() was called
   * @return number of tokens remaining in the bucket
   */
  public long getStaleTokenCount() {
    // TODO: maybe update the token after getting the stale token count
    return tokens.get();
  }

  public boolean tryConsume(long tokensToConsume) {
    tokensRequestedSinceLastRefill.getAndAdd(tokensToConsume);
    if (noRetryTryConsume(tokensToConsume)) {
      return true;
    } else {
      update();
      return noRetryTryConsume(tokensToConsume);
    }
  }

  private boolean noRetryTryConsume(long tokensToConsume) {
    long tokensThatWereAvailable = tokens.getAndAccumulate(tokensToConsume, (existing, toConsume) -> {
      if (toConsume <= existing) { // there are sufficient tokens
        return existing - toConsume;
      } else {
        return existing; // insufficient tokens, do not consume any
      }
    });
    return tokensToConsume <= tokensThatWereAvailable;
  }

  public boolean tryConsume() {
    return tryConsume(1);
  }

  public float getAmortizedRefillPerSecond() {
    return refillPerSecond;
  }

  public double getStaleUsageRatio() {
    long timeSinceLastRefill = TimeUnit.MILLISECONDS.toSeconds(clock.millis() - previousRefillTime);
    if (timeSinceLastRefill > 0) {
      return ((double) tokensRequestedSinceLastRefill.get() / (double) timeSinceLastRefill) / refillPerSecond;
    } else {
      return 0d;
    }
  }
}
