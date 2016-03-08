package com.linkedin.venice.controlmessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Control message used to notify controller that the status of Offline push in Storage node.
 */
public class StatusUpdateMessage extends ControlMessage {
  private static final String MESSAGE_ID = "messageId";
  private static final String PARTITION_ID = "partitionId";
  private static final String KAFKA_TOPIC = "kafkaId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String STATUS = "status";

  private final String messageId;
  private final int partitionId;
  private final String kafkaTopic;
  private final String instanceId;

  private final Status status;

  public StatusUpdateMessage(String kafkaTopic, int partitionId, String instanceId, Status status) {
    this.messageId = UUID.randomUUID().toString();
    this.partitionId = partitionId;
    this.kafkaTopic = kafkaTopic;
    this.instanceId = instanceId;
    this.status = status;
  }

  /**
   * Override the constructor of ControlMessage, build message from given fiedls.
   */
  public StatusUpdateMessage(Map<String, String> fields) {
    this.messageId = getRequiredField(fields, MESSAGE_ID);
    this.partitionId = Integer.valueOf(getRequiredField(fields, PARTITION_ID));
    this.instanceId = getRequiredField(fields, INSTANCE_ID);
    this.kafkaTopic = getRequiredField(fields, KAFKA_TOPIC);
    this.status = Status.valueOf(getRequiredField(fields, STATUS));
  }

  public String getMessageId() {
    return messageId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public Status getStatus() {
    return status;
  }

  @Override
  public Map<String, String> getFields() {
    HashMap<String, String> map = new HashMap<>();
    map.put(MESSAGE_ID, messageId);
    map.put(PARTITION_ID, String.valueOf(partitionId));
    map.put(KAFKA_TOPIC, kafkaTopic);
    map.put(INSTANCE_ID, instanceId);
    map.put(STATUS, status.toString());
    return map;
  }

  /**
   * Status of off-line push in storage node.
   */
  //TODO will add more status or refine the definition here in the further.
  public enum Status {
    //Start consuming data from Kafka
    STARTED,
    //The progress of processing the data.
    PROGRESS,
    //Data is read and put into storage engine.
    COMPLETED,
    //Met error when processing the data.
    //TODO will separate it to different types of error later.
    ERROR
  }
}
