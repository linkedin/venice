package com.linkedin.venice.notifier;

import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixControlMessageChannel;
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

  private static final Logger logger = Logger.getLogger(HelixNotifier.class.getName());

  private final HelixControlMessageChannel messageChannel;
  private final String instanceId;

  public HelixNotifier(HelixManager manager, String instanceId) {
    this.messageChannel = new HelixControlMessageChannel(manager);
    this.instanceId = instanceId;
  }

  private void sendMessage(StoreStatusMessage message) {
    try {
       messageChannel.sendToController(message);
    } catch(IOException ex) {
      String errorMessage = "Error Sending Message to Helix Controller " + message.getMessageId();
      logger.error(errorMessage , ex);
      throw new VeniceException(errorMessage , ex);
    }
  }

  @Override
  public void started(String storeName, int partitionId) {
    StoreStatusMessage
            veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId, ExecutionStatus.STARTED);
    sendMessage(veniceMessage);
  }

  @Override
  public void completed(String storeName, int partitionId, long offset) {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId,
            ExecutionStatus.COMPLETED);
    veniceMessage.setOffset(offset);
    sendMessage(veniceMessage);
  }

  @Override
  public void progress(String storeName, int partitionId, long offset) {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(storeName, partitionId, instanceId,
            ExecutionStatus.PROGRESS);
    veniceMessage.setOffset(offset);
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
    sendMessage(veniceMessage);
  }
}
