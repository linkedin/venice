package com.linkedin.venice.helix;

import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import java.util.List;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Serializer used to convert the data between the list of task and json.
 * <p>
 * In zk, we store tasks which belong to same partition together to avoid creating too many children in ZK.
 */
public class TasksJSONSerializer implements VeniceSerializer<List<Task>> {
  private final ObjectMapper mapper = new ObjectMapper();
  private TypeReference<List<Task>> typeReference;

  public TasksJSONSerializer() {
    mapper.getDeserializationConfig().addMixInAnnotations(Task.class, TasksSerializerMixin.class);
    typeReference = new TypeReference<List<Task>>() {
    };
  }

  @Override
  public byte[] serialize(List<Task> object, String path)
      throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public List<Task> deserialize(byte[] bytes, String path)
      throws IOException {
    return mapper.readValue(bytes, typeReference);
  }

  public static class TasksSerializerMixin {
    @JsonCreator
    public TasksSerializerMixin(@JsonProperty("taskId") String taskId, @JsonProperty("partitionId") int partitionId,
        @JsonProperty("instanceId") String instanceId) {

    }
  }
}
