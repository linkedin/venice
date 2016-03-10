package com.linkedin.venice.controlmessage;

import com.linkedin.venice.job.JobAndTaskStatus;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Control message used to notify controller that the status of Offline push in Storage node.
 */
public class StatusUpdateMessage extends ControlMessage {
  private static final String JOB_ID = "jobId";
  private static final String MESSAGE_ID = "messageId";
  private static final String PARTITION_ID = "partitionId";
  private static final String KAFKA_TOPIC = "kafkaId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String STATUS = "status";

  private final long jobId;
  private final String messageId;
  private final int partitionId;
  private final String kafkaTopic;
  private final String instanceId;

  private final JobAndTaskStatus status;

  public StatusUpdateMessage(long jobId, String kafkaTopic, int partitionId, String instanceId, JobAndTaskStatus status) {
    this.jobId = jobId;
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
    this.jobId = Long.valueOf(getRequiredField(fields, JOB_ID));
    this.messageId = getRequiredField(fields, MESSAGE_ID);
    this.partitionId = Integer.valueOf(getRequiredField(fields, PARTITION_ID));
    this.instanceId = getRequiredField(fields, INSTANCE_ID);
    this.kafkaTopic = getRequiredField(fields, KAFKA_TOPIC);
    this.status = JobAndTaskStatus.valueOf(getRequiredField(fields, STATUS));
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

  @Override
  public Map<String, String> getFields() {
    HashMap<String, String> map = new HashMap<>();
    map.put(JOB_ID, String.valueOf(jobId));
    map.put(MESSAGE_ID, messageId);
    map.put(PARTITION_ID, String.valueOf(partitionId));
    map.put(KAFKA_TOPIC, kafkaTopic);
    map.put(INSTANCE_ID, instanceId);
    map.put(STATUS, status.toString());
    return map;
  }
}
