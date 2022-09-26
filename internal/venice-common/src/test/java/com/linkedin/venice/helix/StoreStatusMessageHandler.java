package com.linkedin.venice.helix;

import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handler in controller side used to deal with status update message from storage node.
 */
public class StoreStatusMessageHandler implements StatusMessageHandler<StoreStatusMessage> {
  private static final Logger LOGGER = LogManager.getLogger(StatusMessageHandler.class);
  // TODO will process the status in the further. Maybe here will become
  // Map<KafkaTopic,Map<Partition,Map<Instance,Status>>>.
  private Map<String, StoreStatusMessage> statusMap;

  public StoreStatusMessageHandler() {
    statusMap = new ConcurrentHashMap<>();
  }

  @Override
  public void handleMessage(StoreStatusMessage message) {
    if (message == null) {
      throw new IllegalArgumentException(" Parameter message is null");
    }
    LOGGER.info("Processing Message {}", message);
    statusMap.put(message.getKafkaTopic(), message);
  }

  // TODO will be changed to get the status from kafkaTopic+partition+instance later.
  public StoreStatusMessage getStatus(String kafkaTopic) {
    return statusMap.get(kafkaTopic);
  }
}
