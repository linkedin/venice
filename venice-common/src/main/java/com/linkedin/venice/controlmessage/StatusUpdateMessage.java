package com.linkedin.venice.controlmessage;

import com.linkedin.venice.job.JobAndTaskStatus;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Control description used to notify controller that the status of Offline push in Storage node.
 */
public class StatusUpdateMessage extends ControlMessage {
  private static final String JOB_ID = "jobId";
  private static final String MESSAGE_ID = "messageId";
  private static final String PARTITION_ID = "partitionId";
  private static final String KAFKA_TOPIC = "kafkaId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String STATUS = "status";
  private static final String OFFSET = "offset";
  private static final String DESCRIPTION = "description";
  private static AtomicInteger counter = new AtomicInteger(0);

  private final long jobId;
  private final String messageId;
  private final int partitionId;
  private final String kafkaTopic;
  private final String instanceId;

  private final JobAndTaskStatus status;

  private long offset;
  private String description;

  public StatusUpdateMessage(long jobId, String kafkaTopic, int partitionId, String instanceId, JobAndTaskStatus status) {

    this.jobId = jobId;
    this.messageId = instanceId + "_" + Integer.toString(counter.getAndIncrement());
    this.partitionId = partitionId;
    this.kafkaTopic = kafkaTopic;
    this.instanceId = instanceId;
    this.status = status;
  }

  /**
   * Override the constructor of ControlMessage, build description from given fiedls.
   */
  public StatusUpdateMessage(Map<String, String> fields) {
    this.jobId = Long.valueOf(getRequiredField(fields, JOB_ID));
    this.messageId = getRequiredField(fields, MESSAGE_ID);
    this.partitionId = Integer.valueOf(getRequiredField(fields, PARTITION_ID));
    this.instanceId = getRequiredField(fields, INSTANCE_ID);
    this.kafkaTopic = getRequiredField(fields, KAFKA_TOPIC);
    this.status = JobAndTaskStatus.valueOf(getRequiredField(fields, STATUS));
    this.description = getOptionalField(fields, DESCRIPTION);
    this.offset = Integer.valueOf(getRequiredField(fields, OFFSET));
  }

  public long getJobId() {
    return jobId;
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

  public JobAndTaskStatus getStatus() {
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
    HashMap<String, String> map = new HashMap<>();
    map.put(JOB_ID, String.valueOf(jobId));
    map.put(MESSAGE_ID, messageId);
    map.put(PARTITION_ID, String.valueOf(partitionId));
    map.put(KAFKA_TOPIC, kafkaTopic);
    map.put(INSTANCE_ID, instanceId);
    map.put(STATUS, status.toString());
    map.put(OFFSET , String.valueOf(offset));

    if(description != null) {
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

    StatusUpdateMessage that = (StatusUpdateMessage) o;

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
    return "StatusUpdateMessage{" +
            "jobId='" + jobId + '\'' +
            ", messageId='" + messageId + '\'' +
            ", partitionId=" + partitionId +
            ", kafkaTopic='" + kafkaTopic + '\'' +
            ", instanceId='" + instanceId + '\'' +
            ", offset=" + offset +
            ", description='" + description + '\'' +
            ", status=" + status +
            '}';
  }
}
