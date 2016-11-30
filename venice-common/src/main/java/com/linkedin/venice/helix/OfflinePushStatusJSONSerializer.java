package com.linkedin.venice.helix;

import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Serializer used to convert the data between {@link OfflinePushStatus} and json.
 */
public class OfflinePushStatusJSONSerializer implements VeniceSerializer<OfflinePushStatus> {
  private final ObjectMapper mapper = new ObjectMapper();

  public OfflinePushStatusJSONSerializer() {
    mapper.getDeserializationConfig()
        .addMixInAnnotations(OfflinePushStatus.class, OfflinePushStatusSerializerMixin.class);
    mapper.getDeserializationConfig().addMixInAnnotations(StatusSnapshot.class, StatusSnapshotSerializerMixin.class);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public byte[] serialize(OfflinePushStatus object, String path)
      throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public OfflinePushStatus deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, OfflinePushStatus.class);
  }

  public static class OfflinePushStatusSerializerMixin {
    @JsonCreator
    public OfflinePushStatusSerializerMixin(@JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("numberOfPartition") int numberOfPartition,
        @JsonProperty("replicationFactor") int replicationFactor,
        @JsonProperty("offlinePushStrategy") OfflinePushStrategy offlinePushStrategy) {
    }
  }

  public static class StatusSnapshotSerializerMixin {
    @JsonCreator
    public StatusSnapshotSerializerMixin(@JsonProperty("status") ExecutionStatus status,
        @JsonProperty("time") String time) {
    }
  }
}
