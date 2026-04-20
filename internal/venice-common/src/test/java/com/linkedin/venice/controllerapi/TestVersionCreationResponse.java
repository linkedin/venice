package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVersionCreationResponse {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void degradedDatacentersSerializationRoundTrip() throws IOException {
    VersionCreationResponse response = new VersionCreationResponse();
    Set<String> dcs = new HashSet<>();
    dcs.add("dc-1");
    dcs.add("dc-2");
    response.setDegradedDatacenters(dcs);

    String json = OBJECT_MAPPER.writeValueAsString(response);
    VersionCreationResponse deserialized = OBJECT_MAPPER.readValue(json, VersionCreationResponse.class);

    Assert.assertNotNull(deserialized.getDegradedDatacenters());
    Assert.assertEquals(deserialized.getDegradedDatacenters().size(), 2);
    Assert.assertTrue(deserialized.getDegradedDatacenters().contains("dc-1"));
    Assert.assertTrue(deserialized.getDegradedDatacenters().contains("dc-2"));
  }

  @Test
  public void oldJsonWithoutDegradedFieldDeserializesCleanly() throws IOException {
    String oldJson = "{\"partitions\":10,\"replicas\":3,\"enableSSL\":false}";
    VersionCreationResponse deserialized = OBJECT_MAPPER.readValue(oldJson, VersionCreationResponse.class);

    Assert.assertNull(deserialized.getDegradedDatacenters());
    Assert.assertEquals(deserialized.getPartitions(), 10);
    Assert.assertEquals(deserialized.getReplicas(), 3);
  }

  @Test
  public void nullDegradedDatacentersIsDefault() throws IOException {
    VersionCreationResponse response = new VersionCreationResponse();
    Assert.assertNull(response.getDegradedDatacenters());

    String json = OBJECT_MAPPER.writeValueAsString(response);
    VersionCreationResponse deserialized = OBJECT_MAPPER.readValue(json, VersionCreationResponse.class);
    Assert.assertNull(deserialized.getDegradedDatacenters());
  }
}
