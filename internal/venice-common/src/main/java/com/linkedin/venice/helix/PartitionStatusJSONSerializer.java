package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;


/**
 * Serializer used to convert the data between {@link PartitionStatus} and json.
 */
public class PartitionStatusJSONSerializer extends VeniceJsonSerializer<PartitionStatus> {
  public PartitionStatusJSONSerializer() {
    super(PartitionStatus.class);
  }

  @Override
  protected void configureObjectMapper(ObjectMapper mapper) {
    mapper.addMixIn(PartitionStatus.class, PartitionStatusSerializerMixin.class);
    mapper.addMixIn(StatusSnapshot.class, OfflinePushStatusJSONSerializer.StatusSnapshotSerializerMixin.class);
    mapper.addMixIn(ReplicaStatus.class, ReplicaStatusSerializerMixin.class);
  }

  public static class PartitionStatusSerializerMixin {
    @JsonCreator
    public PartitionStatusSerializerMixin(@JsonProperty("partitionId") int partitionId) {

    }
  }

  public static class ReplicaStatusSerializerMixin {
    @JsonCreator
    public ReplicaStatusSerializerMixin(@JsonProperty("instanceId") String instanceId) {

    }
  }
}
