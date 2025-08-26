package com.linkedin.davinci.notifier;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {
  private static final Logger LOGGER = LogManager.getLogger(LogNotifier.class);

  @Override
  public void started(String pubSubTopic, int partitionId, String message) {
    logMessage("Push started", pubSubTopic, partitionId, null, message, null);
  }

  @Override
  public void restarted(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Push restarted", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void completed(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Push completed", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void progress(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Push progress", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void endOfPushReceived(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Received END_OF_PUSH", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void topicSwitchReceived(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Received TOPIC_SWITCH", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void dataRecoveryCompleted(String pubSubTopic, int partitionId, PubSubPosition position, String message) {
    logMessage("Data recovery completed", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void startOfIncrementalPushReceived(
      String pubSubTopic,
      int partitionId,
      PubSubPosition position,
      String message) {
    logMessage("Received START_OF_INCREMENTAL_PUSH", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void endOfIncrementalPushReceived(
      String pubSubTopic,
      int partitionId,
      PubSubPosition position,
      String message) {
    logMessage("Received END_OF_INCREMENTAL_PUSH", pubSubTopic, partitionId, position, message, null);
  }

  @Override
  public void catchUpVersionTopicOffsetLag(String pubSubTopic, int partitionId) {
    logMessage("Received CATCH_UP_BASE_TOPIC_OFFSET_LAG", pubSubTopic, partitionId, null, "", null);
  }

  private void logMessage(
      String header,
      String pubSubTopic,
      int partitionId,
      PubSubPosition position,
      String message,
      Exception ex) {
    String logMessageString = String.format(
        "%s for replica: %s%s%s",
        header,
        Utils.getReplicaId(pubSubTopic, partitionId),
        position == null ? "" : " position " + position,
        (message == null || message.isEmpty()) ? "" : " message " + message);
    if (ex == null) {
      LOGGER.info(logMessageString);
    } else {
      LOGGER.error(logMessageString, ex);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String pubSubTopic, int partitionId, String message, Exception ex) {
    logMessage("Push errored", pubSubTopic, partitionId, null, message, ex);
  }

  @Override
  public void stopped(String pubSubTopic, int partitionId, PubSubPosition position) {
    logMessage("Consumption stopped", pubSubTopic, partitionId, position, null, null);
  }
}
