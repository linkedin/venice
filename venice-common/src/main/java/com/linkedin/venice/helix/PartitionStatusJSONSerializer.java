package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Serializer used to convert the data between {@link PartitionStatus} and json.
 */
public class PartitionStatusJSONSerializer extends VeniceJsonSerializer<PartitionStatus> {

  public PartitionStatusJSONSerializer() {
    super(PartitionStatus.class);
    mapper.getDeserializationConfig().addMixInAnnotations(PartitionStatus.class, PartitionStatusSerializerMixin.class);
    mapper.getDeserializationConfig()
        .addMixInAnnotations(StatusSnapshot.class, OfflinePushStatusJSONSerializer.StatusSnapshotSerializerMixin.class);
    mapper.getDeserializationConfig().addMixInAnnotations(ReplicaStatus.class, ReplicaStatusSerializerMixin.class);
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
