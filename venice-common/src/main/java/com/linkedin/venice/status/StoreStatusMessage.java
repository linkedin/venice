package com.linkedin.venice.status;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Map;


/**
 * Control description used to notify controller that the status of Offline push in Storage node.
 *
 * DO NOT CHANGE THE PACKAGE OR CLASS NAME. The class name is used by the
 * {@link com.linkedin.venice.helix.HelixStatusMessageChannel}. When the
 * class name is updated and if the Controller and Storage Node is not
 * updated at the same time, one can't parse the message sent by the other
 * as they embed the name in the message.
 */
public class StoreStatusMessage extends StatusMessage {

  /* DO NOT CHANGE or REMOVE the field names.
  The contents of this class are serialized using JSON
  Serialization on the storage node and sent to the controller.
  Controller uses JSON De-Serializer and uses reflection to
  initialize the members of this object.
  
  Changing the fieldName will not generate compile time error,
  but will cause it to fail on the controller side.
  
  Removing/Adding the field will have the same effect. But can be
  controlled to a certain extent by making the controller deployed
  first to be backward compatible and then deploying the storage
  node.
   */
  private static final String PARTITION_ID = "partitionId";
  private static final String KAFKA_TOPIC = "kafkaTopic";
  private static final String INSTANCE_ID = "instanceId";
  private static final String STATUS = "status";
  private static final String OFFSET = "offset";
  private static final String DESCRIPTION = "description";

  private final int partitionId;
  private final String kafkaTopic;
  private final String instanceId;

  private final ExecutionStatus status;

  private long offset;
  private String description;

  public StoreStatusMessage(String kafkaTopic, int partitionId, String instanceId, ExecutionStatus status) {
    this.partitionId = partitionId;
    this.kafkaTopic = kafkaTopic;
    this.instanceId = instanceId;
    this.status = status;
  }

  /**
   * Override the constructor of {@link StatusMessage}, build description from given fiedls.
   */
  public StoreStatusMessage(Map<String, String> fields) {
    super(fields);
    this.partitionId = Integer.parseInt(getRequiredField(fields, PARTITION_ID));
    this.instanceId = getRequiredField(fields, INSTANCE_ID);
    this.kafkaTopic = getRequiredField(fields, KAFKA_TOPIC);
    this.status = ExecutionStatus.valueOf(getRequiredField(fields, STATUS));
    this.description = getOptionalField(fields, DESCRIPTION);
    this.offset = Integer.parseInt(getRequiredField(fields, OFFSET));
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

  public ExecutionStatus getStatus() {
    return status;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public Map<String, String> getFields() {
    Map<String, String> map = super.getFields();
    map.put(PARTITION_ID, String.valueOf(partitionId));
    map.put(KAFKA_TOPIC, kafkaTopic);
    map.put(INSTANCE_ID, instanceId);
    map.put(STATUS, status.toString());
    map.put(OFFSET, String.valueOf(offset));

    if (description != null) {
      map.put(DESCRIPTION, description);
    }

    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoreStatusMessage that = (StoreStatusMessage) o;

    if (partitionId != that.partitionId) {
      return false;
    }
    if (offset != that.offset) {
      return false;
    }
    if (!messageId.equals(that.messageId)) {
      return false;
    }
    if (!kafkaTopic.equals(that.kafkaTopic)) {
      return false;
    }
    if (!instanceId.equals(that.instanceId)) {
      return false;
    }
    if (description != null ? !description.equals(that.description) : that.description != null) {
      return false;
    }
    return status == that.status;
  }

  @Override
  public int hashCode() {
    return messageId.hashCode();
  }

  @Override
  public String toString() {
    return "StoreStatusMessage{" + ", messageId='" + messageId + '\'' + ", partitionId=" + partitionId
        + ", kafkaTopic='" + kafkaTopic + '\'' + ", instanceId='" + instanceId + '\'' + ", offset=" + offset
        + ", description='" + description + '\'' + ", status=" + status + '}';
  }
}
