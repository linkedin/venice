package com.linkedin.venice.controlmessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Control message used to notify controller that the status of Offline push in Storage node.
 */
public class StatusUpdateMessage implements ControlMessage {
  private static final String MESSAGE_ID = "messageId";
  private static final String PARTITION_ID = "partitionId";
  private static final String KAFKA_TOPIC = "kafkaId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String STATUS = "status";

  private String messageId;
  private int partitionId;
  private String kafkaTopic;
  private String instanceId;

  private Status status;

  public StatusUpdateMessage(String kafkaTopic, int partitionId, String instanceId, Status status) {
    this.messageId = UUID.randomUUID().toString();
    this.partitionId = partitionId;
    this.kafkaTopic = kafkaTopic;
    this.instanceId = instanceId;
    this.status = status;
  }

  /**
   * Constructor used to crate an empty message then fill by properties. Should NOT be used for other purpose.
   */
  public StatusUpdateMessage() {

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

  @Override
  public void buildFromFields(Map<String, String> properties) {
    //TODO validate
    this.messageId = properties.get(MESSAGE_ID);
    this.partitionId = Integer.valueOf(properties.get(PARTITION_ID));
    this.instanceId = properties.get(INSTANCE_ID);
    this.kafkaTopic = properties.get(KAFKA_TOPIC);
    this.status = Status.valueOf(properties.get(STATUS));
  }

  /**
   * Status of off-line push in storage node.
   */
  //TODO will add more status or refine the definition here in the further.
  public enum Status {
    //Data is consumed and put to storage engine correctly, ready to serve.
    FINALIZED,
    //Data is failed in validation steps
    VALIDATE_FAILED,
    //Data can not be read from Kafka correctly.
    READ_FAILED,
    //Data passed validation, but failed to put into storage engine.
    FINALIZE_FAILED
  }
}
