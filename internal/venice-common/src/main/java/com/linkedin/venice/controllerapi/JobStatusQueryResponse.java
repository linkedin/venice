package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Response for querying job status.
 */
public class JobStatusQueryResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private int version;
  private String status;
  private String statusDetails;
  private Long statusUpdateTimestamp;
  private Map<String, String> extraInfo;
  private Map<String, String> extraDetails;
  private Map<String, Long> extraInfoUpdateTimestamp;

  private List<UncompletedPartition> uncompletedPartitions;

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
   * N.B.: Older versions of the controller did not support this timestamp, so this can be null.
   *
   * @return the UNIX Epoch timestamp when the value of {@link #getStatus()} was last updated (for child controllers),
   *         null for parent controllers and for older versions of child controllers.
   */
  public Long getStatusUpdateTimestamp() {
    return this.statusUpdateTimestamp;
  }

  public void setStatusUpdateTimestamp(Long statusUpdateTimestamp) {
    this.statusUpdateTimestamp = statusUpdateTimestamp;
  }

  /**
   * N.B.: The values in this map conform to {@link ExecutionStatus} values.
   *
   * @return A map of region name -> status, which can be returned by a parent controller.
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
   * @return A map of region name -> status details, which can be returned by a parent controller.
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

  /**
   * N.B.: Older versions of the controller did not support these timestamps, so this map can be null.
   *
   * @return a map of region name -> UNIX Epoch timestamp indicating when the value corresponding to the same key in
   *         {@link #getExtraInfo()} was last updated, or null if not available.
   */
  public Map<String, Long> getExtraInfoUpdateTimestamp() {
    return this.extraInfoUpdateTimestamp;
  }

  public void setExtraInfoUpdateTimestamp(Map<String, Long> extraInfoUpdateTimestamp) {
    this.extraInfoUpdateTimestamp = extraInfoUpdateTimestamp;
  }

  public void setUncompletedPartitions(List<UncompletedPartition> uncompletedPartitions) {
    this.uncompletedPartitions = uncompletedPartitions;
  }

  public List<UncompletedPartition> getUncompletedPartitions() {
    return uncompletedPartitions;
  }

  public String toString() {
    return JobStatusQueryResponse.class.getSimpleName() + "(\n" + "version: " + version + ",\n" + "status: " + status
        + ",\n" + "statusDetails: " + statusDetails + ",\n" + "extraInfo: " + extraInfo + ",\n" + "extraDetails: "
        + extraDetails + ",\n" + super.toString() + ")";
  }
}
