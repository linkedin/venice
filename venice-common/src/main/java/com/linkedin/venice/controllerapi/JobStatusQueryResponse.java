package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Map;
import java.util.Optional;


/**
 * Response for querying job status.
 */
public class JobStatusQueryResponse extends ControllerResponse{ /* Uses Json Reflective Serializer, get without set may break things */

  private int version;
  private String status;
  private String statusDetails;
  private Map<String, String> extraInfo;
  private Map<String, String> extraDetails;

  // The folllowing progress info won't be valid any more, and they could be removed eventually in the future.
  private long messagesConsumed;
  private long messagesAvailable;
  private Map<String, Long> perTaskProgress;
  private Map<Integer, Long> perPartitionCapacity;

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
   * N.B.: Older versions of the controller did not support these details, so the optional can be empty.
   */
  @JsonIgnore
  public Optional<String> getOptionalStatusDetails() {
    return Optional.ofNullable(statusDetails);
  }

  /**
   * @deprecated Only used for JSON serialization purposes. Use {@link #getOptionalExtraDetails()} instead.
   */
  @Deprecated
  public String getStatusDetails() {
    return statusDetails;
  }

  public void setStatusDetails(String statusDetails) {
    this.statusDetails = statusDetails;
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

  /**
   * N.B.: The values in this map conform to {@link ExecutionStatus} values.
   *
   * @return A map of datacenter -> status, which can be returned by a parent controller.
   */
  public Map<String, String> getExtraInfo() {
    return extraInfo;
  }

  public void setExtraInfo(Map<String, String> extraInfo) {
    this.extraInfo = extraInfo;
  }

  /**
   * N.B.: Older versions of the controller did not support these details, so the optional can be empty.
   *
   * @return A map of datacenter -> status details, which can be returned by a parent controller.
   */
  @JsonIgnore
  public Optional<Map<String, String>> getOptionalExtraDetails() {
    return Optional.ofNullable(extraDetails);
  }

  /**
   * @deprecated Only used for JSON serialization purposes. Use {@link #getOptionalExtraDetails()} instead.
   */
  @Deprecated
  public Map<String, String> getExtraDetails() {
    return extraDetails;
  }

  public void setExtraDetails(Map<String, String> extraDetails) {
    this.extraDetails = extraDetails;
  }

  public static JobStatusQueryResponse createErrorResponse(String errorMessage) {
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setError(errorMessage);
    return response;
  }

  public String toString() {
    return JobStatusQueryResponse.class.getSimpleName() + "(\n"
        + "version: " + version + ",\n"
        + "status: " + status + ",\n"
        + "statusDetails: " + statusDetails + ",\n"
        + "extraInfo: " + extraInfo + ",\n"
        + "extraDetails: " + extraDetails + ",\n"
        + super.toString() + ")";
  }
}
