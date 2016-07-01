package com.linkedin.venice.helix;

import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
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
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public byte[] serialize(OfflineJob object, String path)
      throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public OfflineJob deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, OfflineJob.class);
  }

  public static class OfflineJobSerializerMixin {
    @JsonCreator
    public OfflineJobSerializerMixin(@JsonProperty("jobId") long jobId, @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("numberOfPartition") int numberOfPartition, @JsonProperty("replicationFactor") int replicationFactor) {

    }
  }
}
