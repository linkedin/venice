package com.linkedin.venice.helix;

import com.linkedin.venice.controlmessage.StatusUpdateMessage;
import com.linkedin.venice.kafka.consumer.VeniceNotifier;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import org.apache.helix.HelixManager;
import org.apache.log4j.Logger;
import org.omg.CORBA.Environment;


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

  private void sendMessage(StatusUpdateMessage message) {
    try {
       messageChannel.sendToController(message);
    } catch(IOException ex) {
      String errorMessage = "Error Sending Message to Helix Controller " + message.getMessageId();
      logger.error(errorMessage , ex);
      throw new RuntimeException(errorMessage , ex);
    }
  }

  @Override
  public void started(long jobId, String storeName, int partitionId) {
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(storeName, partitionId, instanceId,
            StatusUpdateMessage.Status.STARTED);
    sendMessage(veniceMessage);
  }

  @Override
  public void completed(long jobId, String storeName, int partitionId, long offset) {
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(storeName, partitionId, instanceId,
            StatusUpdateMessage.Status.COMPLETED);
    veniceMessage.setOffset(offset);
    sendMessage(veniceMessage);
  }

  @Override
  public void progress(long jobId, String storeName, int partitionId, long offset) {
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(storeName, partitionId, instanceId,
            StatusUpdateMessage.Status.PROGRESS);
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
  public void error(long jobId, String storeName, int partitionId, String message, Exception ex) {
    StatusUpdateMessage veniceMessage =
            new StatusUpdateMessage(storeName, partitionId, instanceId, StatusUpdateMessage.Status.ERROR);
    veniceMessage.setDescription(formatError(message , ex));
    sendMessage(veniceMessage);
  }
}
