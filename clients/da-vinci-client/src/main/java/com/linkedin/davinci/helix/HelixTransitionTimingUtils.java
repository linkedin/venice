package com.linkedin.davinci.helix;

import java.util.StringJoiner;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;


/**
 * Utility class for computing timing metrics for Helix state transition messages.
 * Provides formatted timing deltas to help diagnose performance issues in state transitions.
 *
 * <p><b>Note:</b> Timing deltas may be affected by clock skew in distributed systems.
 * Values of -1 indicate unavailable timestamps.
 */
final class HelixTransitionTimingUtils {
  private HelixTransitionTimingUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Builds a formatted string summarizing key timing deltas for a Helix state transition message.
   * Uses only public Helix APIs.
   *
   * @param message the Helix Message containing timestamp fields
   * @param context the NotificationContext containing callback creation time
   * @return formatted string with timing deltas, ready to append to log messages
   */
  static String formatTransitionTiming(Message message, NotificationContext context) {
    if (message == null) {
      return "[deltas: unavailable - null message]";
    }

    try {
      // Extract timestamps from message and context
      long createTs = safe(message.getCreateTimeStamp());
      long executeStartTs = safe(message.getExecuteStartTimeStamp());
      long readTs = safe(message.getReadTimeStamp());
      long zkCreateTs = (message.getStat() != null) ? safe(message.getStat().getCreationTime()) : UNAVAILABLE;
      long contextCreateTs = (context != null) ? safe(context.getCreationTime()) : UNAVAILABLE;
      long now = System.currentTimeMillis();

      // Compute timing deltas
      // Controller -> execution start: total propagation + scheduling delay
      long createToExecuteMs = delta(executeStartTs, createTs);

      // ZK -> execution start: ZooKeeper and participant-side delay
      long zkCreateToExecuteMs = delta(executeStartTs, zkCreateTs);

      // Controller -> participant read: propagation time across controller -> ZK -> participant
      long createToReadMs = delta(readTs, createTs);

      // Execution start -> now: how long the transition has been in progress
      long sinceExecuteMs = delta(now, executeStartTs);

      // Controller creation -> now: total lifetime of the message
      long endToEndDurationMs = delta(now, createTs);

      // NotificationContext creation -> now: how long ago Helix detected the event
      long contextAgeMs = delta(now, contextCreateTs);

      // Participant read -> execution start: queuing delay (executor backlog)
      long queueDelayMs = delta(executeStartTs, readTs);

      // Controller -> ZK: controller-side enqueue or ZooKeeper write latency
      long controllerToZkDelayMs = delta(zkCreateTs, createTs);

      // ZK -> participant read: notification propagation through ZK watchers
      long zkToReadDelayMs = delta(readTs, zkCreateTs);

      // Participant read -> NotificationContext: internal framework scheduling delays
      long contextLagMs = delta(contextCreateTs, readTs);

      return String.format(
          "[controllerToZkDelayMs=%d, zkToReadDelayMs=%d, createToReadMs=%d, "
              + "queueDelayMs=%d, createToExecuteMs=%d, zkCreateToExecuteMs=%d, "
              + "sinceExecuteMs=%d, contextLagMs=%d, contextAgeMs=%d, endToEndDurationMs=%d]",
          controllerToZkDelayMs, // controller -> ZK (write latency)
          zkToReadDelayMs, // ZK -> participant read (watcher delay)
          createToReadMs, // controller -> participant read (overall propagation)
          queueDelayMs, // participant read -> execution start (thread queue)
          createToExecuteMs, // controller -> execution start (aggregate)
          zkCreateToExecuteMs, // ZK -> execution start (post-ZK propagation)
          sinceExecuteMs, // execution start -> now (runtime duration)
          contextLagMs, // participant read -> NotificationContext creation
          contextAgeMs, // NotificationContext creation -> now
          endToEndDurationMs // controller creation -> now (end-to-end transition duration)
      );
    } catch (Exception e) {
      return "[deltas: unavailable due to error: " + e.getMessage() + "]";
    }
  }

  /** Sentinel value indicating timestamp is unavailable. */
  private static final long UNAVAILABLE = -1L;

  /**
   * Compute a delta if both timestamps are valid; otherwise return UNAVAILABLE.
   *
   * @param end the end timestamp
   * @param start the start timestamp
   * @return the difference (end - start) if both are valid, otherwise UNAVAILABLE
   */
  private static long delta(long end, long start) {
    return (end != UNAVAILABLE && start != UNAVAILABLE) ? (end - start) : UNAVAILABLE;
  }

  /**
   * Normalize invalid timestamps (≤ 0) to UNAVAILABLE for consistent handling.
   *
   * @param ts the timestamp to normalize
   * @return the timestamp if positive, otherwise UNAVAILABLE
   */
  private static long safe(long ts) {
    return ts > 0 ? ts : UNAVAILABLE;
  }

  /**
   * Formats NotificationContext for better logging readability.
   * Extracts key information like change type, event details, and manager information.
   *
   * @param context the NotificationContext to format
   * @return formatted string representation of the context
   */
  static String formatNotificationContext(NotificationContext context) {
    if (context == null) {
      return "null";
    }

    StringJoiner joiner = new StringJoiner(", ", "{", "}");

    // Event name (e.g., CurrentStateChange, IdealStateChange)
    if (context.getEventName() != null) {
      joiner.add("eventName=" + context.getEventName());
    }

    // Notification type (INIT, CALLBACK, PERIODIC_REFRESH, FINALIZE)
    if (context.getType() != null) {
      joiner.add("type=" + context.getType());
    }

    // Change type (e.g., IDEAL_STATE, CURRENT_STATE, EXTERNAL_VIEW, etc.)
    if (context.getChangeType() != null) {
      joiner.add("changeType=" + context.getChangeType());
    }

    // Path changed in ZooKeeper
    if (context.getPathChanged() != null) {
      joiner.add("pathChanged=" + context.getPathChanged());
    }

    // Creation time and age
    long creationTime = context.getCreationTime();
    long age = System.currentTimeMillis() - creationTime;
    joiner.add("creationTime=" + creationTime);
    joiner.add("ageMs=" + age);

    // Child change flag
    if (context.getIsChildChange()) {
      joiner.add("isChildChange=true");
    }

    // Manager information
    if (context.getManager() != null) {
      joiner.add("instanceName=" + context.getManager().getInstanceName());
      joiner.add("clusterName=" + context.getManager().getClusterName());
      joiner.add("sessionId=" + context.getManager().getSessionId());
    }

    return joiner.toString();
  }
}
