package com.linkedin.venice.controlmessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


/**
 * Handler in controller side used to deal with status update message from storage node.
 */
public class StatusUpdateMessageHandler implements ControlMessageHandler<StatusUpdateMessage> {
  private static final Logger logger = Logger.getLogger(ControlMessageHandler.class);
  //TODO will process the status in the further. Maybe here will become Map<KafkaTopic,Map<Partition,Map<Instance,Status>>>.
  private Map<String, StatusUpdateMessage> statusMap;

  public StatusUpdateMessageHandler() {
    statusMap = new ConcurrentHashMap<>();
  }

  @Override
  public void handleMessage(StatusUpdateMessage message) {
    logger.info("Get message:"+message.getMessageId());
    for (Map.Entry<String, String> entry : message.getFields().entrySet()) {
      logger.debug(entry.getKey() + ":" + entry.getValue() + ";");
    }
    statusMap.put(message.getKafkaTopic(), message);
  }

  //TODO will be changed to get the status from kafkaTopic+partition+instance later.
  public StatusUpdateMessage getStatus(String kafkaTopic) {
    return statusMap.get(kafkaTopic);
  }
}
