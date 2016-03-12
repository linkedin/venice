package com.linkedin.venice.helix;

import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Serializer used to convert the data between Job and json.
 * <p>
 * As task will be stored in ZK separately, so do not serialize/deserialize tasks here.
 */
public class OfflineJobJSONSerializer implements VeniceSerializer<OfflineJob> {
  private final ObjectMapper mapper = new ObjectMapper();

  public OfflineJobJSONSerializer() {
    mapper.getDeserializationConfig().addMixInAnnotations(OfflineJob.class, OfflineJobSerializerMixin.class);
  }

  @Override
  public byte[] serialize(OfflineJob object)
      throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public OfflineJob deserialize(byte[] bytes)
      throws IOException {
    return mapper.readValue(bytes, OfflineJob.class);
  }

  public static class OfflineJobSerializerMixin {
    @JsonCreator
    public OfflineJobSerializerMixin(@JsonProperty("jobId") long jobId, @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("numberOfPartition") int numberOfPartition, @JsonProperty("replicaFactor") int replicaFactor) {

    }
  }
}
