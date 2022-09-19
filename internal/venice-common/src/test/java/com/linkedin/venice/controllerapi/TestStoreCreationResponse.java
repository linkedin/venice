package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
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

  static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void creationResponseCanBeSerialized() throws IOException {
    VersionCreationResponse obj = new VersionCreationResponse();
    obj.setName(STORENAME);
    obj.setKafkaBootstrapServers(KAFKA);
    obj.setKafkaTopic(TOPIC);

    String serialized = OBJECT_MAPPER.writeValueAsString(obj);

    VersionCreationResponse deserialized = OBJECT_MAPPER.readValue(serialized, VersionCreationResponse.class);

    Assert.assertEquals(deserialized.getName(), STORENAME);
    Assert.assertEquals(deserialized.getKafkaBootstrapServers(), KAFKA);
    Assert.assertEquals(deserialized.getKafkaTopic(), TOPIC);
    Assert.assertFalse(deserialized.isError());
  }
}
