package com.linkedin.venice.controlmessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Handler in controller side used to deal with status update message from storage node.
 */
public class StatusUpdateMessageHandler implements ControlMessageHandler<StatusUpdateMessage> {
  //TODO will process the status in the further. Maybe here will become Map<KafkaTopic,Map<Partition,Map<Instance,Status>>>.
  private Map<String, StatusUpdateMessage> statusMap;

  public StatusUpdateMessageHandler() {
    statusMap = new ConcurrentHashMap<>();
  }

  @Override
  public void handleMessage(StatusUpdateMessage message) {
    System.out.println("Get: message");
    for (Map.Entry<String, String> entry : message.getFields().entrySet()) {
      System.out.printf(entry.getKey() + ":" + entry.getValue() + ";");
    }
    statusMap.put(message.getKafkaTopic(), message);
  }

  //TODO will be changed to get the status from kafkaTopic+partition+instance later.
  public StatusUpdateMessage getStatus(String kafkaTopic) {
    return statusMap.get(kafkaTopic);
  }
}
