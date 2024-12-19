package com.linkedin.venice.router.api.routing.helix;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.stats.HelixGroupStats;
import com.linkedin.venice.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This strategy is tried to distribute more load to Helix Groups with more capacity.
 * Since the qps to a specific Router is not that high, so it is acceptable to use synchronized method
 * here.
 * TODO: if we notice a performance issue with the synchronized implementation, we could tune it better
 * to use Atomic data structures by sacrificing the accuracy.
 *
 * This class is also leveraging {@link TimeoutProcessor} to handle potential group counter leaking issue.
 */
public class HelixGroupLeastLoadedStrategy implements HelixGroupSelectionStrategy {
  private static final Logger LOGGER = LogManager.getLogger(HelixGroupLeastLoadedStrategy.class);

  public static final int MAX_ALLOWED_GROUP = 100;
  private final int[] counters = new int[MAX_ALLOWED_GROUP];
  private final TimeoutProcessor timeoutProcessor;
  private final long timeoutInMS;
  private final Map<Long, Pair<Integer, TimeoutProcessor.TimeoutFuture>> requestTimeoutFutureMap = new HashMap<>();
  private final HelixGroupStats helixGroupStats;

  public HelixGroupLeastLoadedStrategy(
      TimeoutProcessor timeoutProcessor,
      long timeoutInMS,
      HelixGroupStats helixGroupStats) {
    this.timeoutProcessor = timeoutProcessor;
    this.timeoutInMS = timeoutInMS;
    this.helixGroupStats = helixGroupStats;
  }

  @Override
  public int selectGroup(long requestId, int groupCount) {
    if (groupCount > MAX_ALLOWED_GROUP || groupCount <= 0) {
      throw new VeniceException(
          "The valid group num must fail into this range: [1, " + MAX_ALLOWED_GROUP + "], but received: " + groupCount);
    }
    int smallestCounter = Integer.MAX_VALUE;
    double lowestAvgLatency = Double.MAX_VALUE;
    int leastLoadedGroup = 0;
    int startGroupId = (int) (requestId % groupCount);
    /**
     * The modification to the group counters should be synchronized to be accurate.
     * If there is a perf issue with this approach, sacrificing accuracy for perf is acceptable.
     */
    synchronized (this) {
      if (requestTimeoutFutureMap.containsKey(requestId)) {
        throw new VeniceException(
            "One request should at most select one group, but request with request id: " + requestId
                + " has invoked this function more than once");
      }
      for (int i = 0; i < groupCount; ++i) {
        int currentGroup = (i + startGroupId) % groupCount;
        int currentGroupCounter = counters[currentGroup];
        if (currentGroupCounter < smallestCounter) {
          smallestCounter = currentGroupCounter;
          leastLoadedGroup = currentGroup;
          lowestAvgLatency = helixGroupStats.getGroupResponseWaitingTimeAvg(currentGroup);
        } else if (currentGroupCounter == smallestCounter) {
          double currentGroupAvgLatency = helixGroupStats.getGroupResponseWaitingTimeAvg(currentGroup);
          /**
           * Here we don't check whether {@link #currentGroupAvgLatency} is less than 0 or not, as when the group is not
           * being used at all, the average latency will be -1.0.
           */
          if (currentGroupAvgLatency < lowestAvgLatency) {
            lowestAvgLatency = currentGroupAvgLatency;
            leastLoadedGroup = currentGroup;
          }
        }
      }
      final int finalLeastLoadedGroup = leastLoadedGroup;
      /**
       * Setting up timeout future for this request since it is possible in some situation, {@link #finishRequest} may
       * not be invoked, and without timeout, the group counter will be leaking.
       */
      requestTimeoutFutureMap.put(
          requestId,
          new Pair<>(
              leastLoadedGroup,
              timeoutProcessor.schedule(
                  () -> timeoutRequest(requestId, finalLeastLoadedGroup, false),
                  timeoutInMS,
                  TimeUnit.MILLISECONDS)));

      ++counters[leastLoadedGroup];
    }
    helixGroupStats.recordGroupPendingRequest(leastLoadedGroup, counters[leastLoadedGroup]);

    return leastLoadedGroup;
  }

  /**
   * Reset the group counter for the specified request.
   * @param requestId
   * @param groupId
   * @param cancelTimeoutFuture
   *          true : for the regular request completion.
   *          false : for timeout scheduler.
   */
  private void timeoutRequest(long requestId, int groupId, boolean cancelTimeoutFuture) {
    if (groupId >= MAX_ALLOWED_GROUP || groupId < 0) {
      throw new VeniceException(
          "The allowed group id must fail into this range: [0, " + (MAX_ALLOWED_GROUP - 1) + "], but received: "
              + groupId);
    }
    if (!cancelTimeoutFuture) {
      // Timeout request
      helixGroupStats.recordGroupResponseWaitingTime(groupId, timeoutInMS);
    }
    synchronized (this) {
      Pair<Integer, TimeoutProcessor.TimeoutFuture> timeoutFuturePair = requestTimeoutFutureMap.get(requestId);
      if (timeoutFuturePair == null) {
        /**
         * Request has already timed out or already finished.
         */
        return;
      }
      if (groupId != timeoutFuturePair.getFirst()) {
        throw new VeniceException(
            "Group id for request with id: " + requestId + " should be: " + timeoutFuturePair.getFirst()
                + ", but received: " + groupId);
      }
      if (--counters[groupId] < 0) {
        counters[groupId] = 0;
        throw new VeniceException(
            "The counter for group: " + groupId + " became negative, something wrong happened, will reset it to be 0.");
      }
      if (cancelTimeoutFuture) {
        // Cancel the timeout future
        timeoutFuturePair.getSecond().cancel();
      } else {
        LOGGER.info(
            "Request with id: {} has timed out with threshold: {}ms, and the counter of group: {} will be reset for this request",
            requestId,
            timeoutInMS,
            groupId);
      }
      requestTimeoutFutureMap.remove(requestId);
    }
  }

  @Override
  public void finishRequest(long requestId, int groupId, double latency) {
    timeoutRequest(requestId, groupId, true);
    helixGroupStats.recordGroupResponseWaitingTime(groupId, latency);
  }
}
