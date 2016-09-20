package com.linkedin.venice.notifier;

import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.helix.HelixManager;
import org.apache.log4j.Logger;


/**
 * Notifier that informs Helix Controller on the status of Consumption tasks.
 */
public class HelixNotifier implements VeniceNotifier {

  private static final Logger logger = Logger.getLogger(HelixNotifier.class);

  private final HelixStatusMessageChannel messageChannel;
  private final String instanceId;
  private final int retryCount;
  private final long retryDurationMs;

  public HelixNotifier(HelixManager manager, String instanceId, int retryCount, long retryDurationMs) {
    this.messageChannel = new HelixStatusMessageChannel(manager);
    this.instanceId = instanceId;
    this.retryCount = retryCount;
    this.retryDurationMs = retryDurationMs;
  }

  private void sendMessage(StoreStatusMessage message) {
    sendMessageWithRetry(message, 0, 0);
  }

  private void sendMessageWithRetry(StoreStatusMessage message, int retryCount, long retryDurationMs) {
    try {
      messageChannel.sendToController(message,retryCount, retryDurationMs);
    } catch(Exception ex) {
      String errorMessage = "Error Sending Message to Helix Controller " + message.getMessageId();
      logger.error(errorMessage , ex);
      throw new VeniceException(errorMessage , ex);
    }
  }

  @Override
  public void started(String storeName, int partitionId) {
    StoreStatusMessage
            veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId, ExecutionStatus.STARTED);
    sendMessageWithRetry(veniceMessage, retryCount, retryDurationMs);
  }

  @Override
  public void restarted(String storeName, int partitionId, long offset) {
    //In terms of task status, restart and start are same behavior.
    started(storeName, partitionId);
  }

  @Override
  public void completed(String storeName, int partitionId, long offset) {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId,
            ExecutionStatus.COMPLETED);
    veniceMessage.setOffset(offset);
    sendMessageWithRetry(veniceMessage, retryCount, retryDurationMs);
  }

  @Override
  public void progress(String storeName, int partitionId, long offset) {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId,
            ExecutionStatus.PROGRESS);
    veniceMessage.setOffset(offset);
    //Do not need to retry, because losing a progress message will not affect the whole job status.
    sendMessage(veniceMessage);
  }

  @Override
  public void close() {
    // This object does not own the Helix Manager, so ignore it.
  }

  private String formatError(String message, Exception ex) {
    StringBuilder sb = new StringBuilder();
    if(message != null) {
      sb.append("Message ");
      sb.append(message);
      sb.append("\n");
    }
    if(ex != null) {
      sb.append("Exception Message ");
      sb.append(ex.getMessage());
      sb.append("\n");
      sb.append(" Stack Trace ");
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      sb.append(sw.toString());
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public void error(String storeName, int partitionId, String message, Exception ex) {
    StoreStatusMessage veniceMessage =
            new StoreStatusMessage(storeName, partitionId, instanceId, ExecutionStatus.ERROR);
    veniceMessage.setDescription(formatError(message , ex));
    sendMessageWithRetry(veniceMessage, retryCount, retryDurationMs);
  }
}
