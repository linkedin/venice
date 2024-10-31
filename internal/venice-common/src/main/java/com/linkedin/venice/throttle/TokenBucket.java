package com.linkedin.venice.throttle;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @see <a href="http://en.wikipedia.org/wiki/Token_bucket">Token Bucket on Wikipedia</a>
 * This implementation aims to be very high performance with the goal of supporting a very large number of
 * TokenBuckets in an application; thus avoiding an auxilliary thread to refill the bucket.
 */
public class TokenBucket implements VeniceRateLimiter {
  private final long capacity;
  private final long refillAmount;
  private final long refillIntervalMs;
  private final float refillPerSecond;// only used for logging
  private final Clock clock;
  private final AtomicLong tokens;
  private final AtomicLong tokensRequestedSinceLastRefill;
  private volatile long previousRefillTime;
  private volatile long nextUpdateTime;

  // Only used in helper methods
  private long quota;

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

  public float getAmortizedRefillPerSecond() {
    return refillPerSecond;
  }

  public double getStaleUsageRatio() {
    long timeSinceLastRefill = MILLISECONDS.toSeconds(clock.millis() - previousRefillTime);
    if (timeSinceLastRefill > 0) {
      return ((double) tokensRequestedSinceLastRefill.get() / (double) timeSinceLastRefill) / refillPerSecond;
    } else {
      return 0d;
    }
  }

  @Override
  public boolean tryAcquirePermit(int units) {
    return tryConsume(units);
  }

  /**
   * @see TokenBucket for an explanation of TokenBucket capacity, refill amount, and refill interval
   *
   * This method takes a rate in units per second, and scales it according to the desired refill interval in milliseconds,
   * and the multiple of refill we wish to use for capacity. Since the resulting token bucket must have a whole number of
   * tokens in its refill amount and capacity, if we only accept one parameter in this method (localRcuPerSecond) then
   * we might have suboptimal results given integer division. For example, with a global rate of 1 Rcu per second, and
   * a proportion of 0.1, along with an enforcement interval of 10 seconds (10000 milliseconds), we would be forced to pass
   * 1 localRcuPerSecond, which is 10x the desired quota. By passing both the global rate of 1 Rcu per second and a desired
   * proportion, this method can multiply by the enforcement interval first before shrinking by the proportion, allowing the
   * Bucket to be configured with the correct 1 refill every 10 seconds.
   *
   * @param totalRcuPerSecond  Number of units per second to allow
   * @param thisBucketProportionOfTotalRcu For maximum fidelity of calculations. If you need a smaller portion of the
   *                                       RCU to be applied then set this to an appropriate multiplier. Otherwise set
   *                                       this to 1.
   * @return
   */
  public static TokenBucket tokenBucketFromRcuPerSecond(
      long totalRcuPerSecond,
      double thisBucketProportionOfTotalRcu,
      long enforcementIntervalMilliseconds,
      int enforcementCapacityMultiple,
      Clock clock) {

    // Calculate total units allowed for the enforcement interval in milliseconds
    double totalRcuPerMillisecond = totalRcuPerSecond / 1000.0;
    long totalRefillAmount = (long) Math.ceil(totalRcuPerMillisecond * enforcementIntervalMilliseconds);

    // Calculate total capacity in terms of the multiple of refill
    long totalCapacity = totalRefillAmount * enforcementCapacityMultiple;

    // Proportionally adjust refill and capacity for this bucket
    long thisRefillAmount = (long) Math.ceil(totalRefillAmount * thisBucketProportionOfTotalRcu);
    long thisCapacity = (long) Math.ceil(totalCapacity * thisBucketProportionOfTotalRcu);

    // Return a token bucket configured with millisecond-based enforcement
    return new TokenBucket(thisCapacity, thisRefillAmount, enforcementIntervalMilliseconds, MILLISECONDS, clock);
  }

  @Override
  public void setQuota(long quota) {
    this.quota = quota;
  }

  @Override
  public long getQuota() {
    return quota;
  }

  @Override
  public String toString() {
    return "TokenBucket{" + "capacity=" + capacity + ", refillAmount=" + refillAmount + ", refillIntervalMs="
        + refillIntervalMs + ", refillPerSecond=" + refillPerSecond + ", tokens=" + tokens
        + ", tokensRequestedSinceLastRefill=" + tokensRequestedSinceLastRefill + ", previousRefillTime="
        + previousRefillTime + ", nextUpdateTime=" + nextUpdateTime + '}';
  }

  public long getCapacity() {
    return capacity;
  }

  public long getRefillAmount() {
    return refillAmount;
  }

  public long getEnforcementInterval() {
    return refillIntervalMs;
  }
}
