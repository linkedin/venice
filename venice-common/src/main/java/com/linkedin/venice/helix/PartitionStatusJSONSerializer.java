package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Serializer used to convert the data between {@link PartitionStatus} and json.
 */
public class PartitionStatusJSONSerializer implements VeniceSerializer<PartitionStatus> {
  private final ObjectMapper mapper = new ObjectMapper();

  public PartitionStatusJSONSerializer() {
    mapper.getDeserializationConfig().addMixInAnnotations(PartitionStatus.class, PartitionStatusSerializerMixin.class);
    mapper.getDeserializationConfig()
        .addMixInAnnotations(StatusSnapshot.class, OfflinePushStatusJSONSerializer.StatusSnapshotSerializerMixin.class);
    mapper.getDeserializationConfig().addMixInAnnotations(ReplicaStatus.class, ReplicaStatusSerializerMixin.class);

    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public byte[] serialize(PartitionStatus object, String path)
      throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public PartitionStatus deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, PartitionStatus.class);
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
