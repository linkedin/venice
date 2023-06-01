package com.linkedin.venice.routerapi;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.io.IOException;
import org.testng.annotations.Test;


public class ReplicaStateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void test() throws IOException {
    ReplicaState replicaState = new ReplicaState(1, "host1234:1234", ExecutionStatus.COMPLETED);
    String json = OBJECT_MAPPER.writeValueAsString(replicaState);
    ReplicaState deserializedReplicaState = OBJECT_MAPPER.readValue(json.getBytes(), ReplicaState.class);
    assertEquals(deserializedReplicaState, replicaState);
  }
}
