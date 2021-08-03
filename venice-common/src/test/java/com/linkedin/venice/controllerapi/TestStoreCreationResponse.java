package com.linkedin.venice.controllerapi;

import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/29/16.
 */
public class TestStoreCreationResponse {
  static final String STORENAME = "mystore";
  static final String OWNER = "dev";
  static final String KAFKA = "localhost:9092";
  static final String TOPIC = "mystore_v3";
  static final int CURRENT_VERSION = 1;

  static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void creationResponseCanBeSerialized()
      throws IOException {
    VersionCreationResponse obj = new VersionCreationResponse();
    obj.setName(STORENAME);
    obj.setKafkaBootstrapServers(KAFKA);
    obj.setKafkaTopic(TOPIC);
    obj.setCurrentVersion(CURRENT_VERSION);

    String serialized = mapper.writeValueAsString(obj);

    VersionCreationResponse deserialized = mapper.readValue(serialized, VersionCreationResponse.class);

    Assert.assertEquals(deserialized.getName(), STORENAME);
    Assert.assertEquals(deserialized.getKafkaBootstrapServers(), KAFKA);
    Assert.assertEquals(deserialized.getKafkaTopic(), TOPIC);
    Assert.assertFalse(deserialized.isError());
    Assert.assertEquals(deserialized.getCurrentVersion(), CURRENT_VERSION);
  }
}
