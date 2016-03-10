package com.linkedin.venice.helix;

import com.linkedin.venice.controlmessage.StatusUpdateMessage;
import com.linkedin.venice.job.JobAndTaskStatus;
import com.linkedin.venice.kafka.consumer.VeniceNotifier;
import java.io.IOException;
import java.util.Collection;
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
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(jobId, storeName, partitionId, instanceId, JobAndTaskStatus.STARTED);
    sendMessage(veniceMessage);
  }

  @Override
  public void completed(long jobId, String storeName, int partitionId, long counter) {
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(jobId, storeName, partitionId, instanceId,
            JobAndTaskStatus.COMPLETED);
    sendMessage(veniceMessage);
  }

  @Override
  public void progress(long jobId, String storeName, int partitionId, long counter) {
    StatusUpdateMessage veniceMessage = new StatusUpdateMessage(jobId, storeName, partitionId, instanceId,
        JobAndTaskStatus.PROGRESS);
    sendMessage(veniceMessage);
  }

  @Override
  public void close() {
    // This object does not own the Helix Manager, so ignore it.
  }

  @Override
  public void error(long jobId, String storeName, Collection<Integer> partitions, String message, Exception ex) {
    for(Integer partitionId : partitions) {
      StatusUpdateMessage veniceMessage =
              new StatusUpdateMessage(jobId, storeName, partitionId, instanceId, JobAndTaskStatus.ERROR);
      sendMessage(veniceMessage);
    }
  }
}
