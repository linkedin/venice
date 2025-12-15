package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;


/**
 * Serializer used to convert the data between {@link OfflinePushStatus} and json.
 */
public class OfflinePushStatusJSONSerializer extends VeniceJsonSerializer<OfflinePushStatus> {
  public OfflinePushStatusJSONSerializer() {
    super(OfflinePushStatus.class);
  }

  @Override
  protected ObjectMapper createObjectMapper() {
    ObjectMapper mapper = super.createObjectMapper();
    mapper.addMixIn(OfflinePushStatus.class, OfflinePushStatusSerializerMixin.class);
    mapper.addMixIn(StatusSnapshot.class, StatusSnapshotSerializerMixin.class);
    return mapper;
  }

  public static class OfflinePushStatusSerializerMixin {
    @JsonCreator
    public OfflinePushStatusSerializerMixin(
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("numberOfPartition") int numberOfPartition,
        @JsonProperty("replicationFactor") int replicationFactor,
        @JsonProperty("offlinePushStrategy") OfflinePushStrategy offlinePushStrategy) {
    }
  }

  public static class StatusSnapshotSerializerMixin {
    @JsonCreator
    public StatusSnapshotSerializerMixin(
        @JsonProperty("status") ExecutionStatus status,
        @JsonProperty("time") String time) {
    }
  }
}
