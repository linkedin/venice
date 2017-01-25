package com.linkedin.venice.controllerapi;

import java.util.Map;


/**
 * Response for querying job status.
 */
public class JobStatusQueryResponse extends ControllerResponse{ /* Uses Json Reflective Serializer, get without set may break things */

  private int version;
  private String status;
  private long messagesConsumed;
  private long messagesAvailable;
  private Map<String, Long> perTaskProgress;
  private Map<Integer, Long> perPartitionCapacity;
  private boolean availableFinal;
  private Map<String, String> extraInfo;

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  /**
   * Aggregate number of kafka offsets consumed by all storage nodes for this store version
   * @return
   */
  public long getMessagesConsumed() {
    return messagesConsumed;
  }

  public void setMessagesConsumed(long messagesConsumed) {
    this.messagesConsumed = messagesConsumed;
  }

  /**
   * Current aggregate number of kafka offsets available to storage nodes for this store version.
   * Effectively this is the sum of the highest offset for each partition times the Venice replication factor
   * @return
   */
  public long getMessagesAvailable() {
    return messagesAvailable;
  }

  public void setMessagesAvailable(long messagesAvailable) {
    this.messagesAvailable = messagesAvailable;
  }

  /**
   * If the push to Kafka is complete, then the highest offset for each partition is not expected to change.
   * This boolean indicates that completion.
   * @return
   */
  public boolean isAvailableFinal() {
    return availableFinal;
  }

  public void setAvailableFinal(boolean availableFinal) {
    this.availableFinal = availableFinal;
  }

  public Map<String, Long> getPerTaskProgress() {
    return perTaskProgress;
  }

  public void setPerTaskProgress(Map<String, Long> perTaskProgress) {
    this.perTaskProgress = perTaskProgress;
  }

  public Map<Integer, Long> getPerPartitionCapacity() {
    return perPartitionCapacity;
  }

  public void setPerPartitionCapacity(Map<Integer, Long> perPartitionCapacity) {
    this.perPartitionCapacity = perPartitionCapacity;
  }

  public Map<String, String> getExtraInfo() {
    return extraInfo;
  }

  public void setExtraInfo(Map<String, String> extraInfo) {
    this.extraInfo = extraInfo;
  }

  public static JobStatusQueryResponse createErrorResponse(String errorMessage) {
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setError(errorMessage);
    return response;
  }
}
