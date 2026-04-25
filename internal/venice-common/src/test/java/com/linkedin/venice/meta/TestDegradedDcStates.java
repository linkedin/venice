package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDegradedDcStates {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void serializationRoundTrip() throws IOException {
    DegradedDcStates states = new DegradedDcStates();
    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(1710000000000L, 120, "operator@test.com"));
    states.addDegradedDatacenter("dc-2", new DegradedDcInfo(1710000060000L, 60, "admin@test.com"));

    String json = OBJECT_MAPPER.writeValueAsString(states);
    DegradedDcStates deserialized = OBJECT_MAPPER.readValue(json, DegradedDcStates.class);

    Assert.assertNotNull(deserialized.getDegradedDatacenters());
    Assert.assertEquals(deserialized.getDegradedDatacenters().size(), 2);
    Assert.assertTrue(deserialized.isDatacenterDegraded("dc-1"));
    Assert.assertTrue(deserialized.isDatacenterDegraded("dc-2"));
    Assert.assertFalse(deserialized.isDatacenterDegraded("dc-3"));

    DegradedDcInfo dc1Info = deserialized.getDegradedDatacenters().get("dc-1");
    Assert.assertEquals(dc1Info.getTimestamp(), 1710000000000L);
    Assert.assertEquals(dc1Info.getTimeoutMinutes(), 120);
    Assert.assertEquals(dc1Info.getOperatorId(), "operator@test.com");
  }

  @Test
  public void emptyStateSerializesCleanly() throws IOException {
    DegradedDcStates states = new DegradedDcStates();
    Assert.assertTrue(states.isEmpty());

    String json = OBJECT_MAPPER.writeValueAsString(states);
    DegradedDcStates deserialized = OBJECT_MAPPER.readValue(json, DegradedDcStates.class);
    Assert.assertTrue(deserialized.isEmpty());
    Assert.assertTrue(deserialized.getDegradedDatacenterNames().isEmpty());
  }

  @Test
  public void copyConstructorDeepCopies() {
    DegradedDcStates original = new DegradedDcStates();
    original.addDegradedDatacenter("dc-1", new DegradedDcInfo(1710000000000L, 120, "operator@test.com"));

    DegradedDcStates copy = new DegradedDcStates(original);
    Assert.assertEquals(copy.getDegradedDatacenters().size(), 1);
    Assert.assertTrue(copy.isDatacenterDegraded("dc-1"));

    // Verify deep copy — modifying copy doesn't affect original
    copy.addDegradedDatacenter("dc-2", new DegradedDcInfo(0, 60, "other"));
    Assert.assertEquals(original.getDegradedDatacenters().size(), 1);
    Assert.assertEquals(copy.getDegradedDatacenters().size(), 2);
  }

  @Test
  public void addAndRemoveDatacenter() {
    DegradedDcStates states = new DegradedDcStates();
    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(1710000000000L, 120, "operator@test.com"));
    Assert.assertTrue(states.isDatacenterDegraded("dc-1"));
    Assert.assertFalse(states.isEmpty());

    states.removeDegradedDatacenter("dc-1");
    Assert.assertFalse(states.isDatacenterDegraded("dc-1"));
    Assert.assertTrue(states.isEmpty());

    // Removing non-existent is safe
    states.removeDegradedDatacenter("dc-nonexistent");
  }

  @Test
  public void nullDatacentersMapHandledGracefully() throws IOException {
    // Jackson can deserialize {"degradedDatacenters": null} — verify all methods handle it
    String json = "{\"degradedDatacenters\": null}";
    DegradedDcStates states = OBJECT_MAPPER.readValue(json, DegradedDcStates.class);
    Assert.assertTrue(states.isEmpty());
    Assert.assertFalse(states.isDatacenterDegraded("dc-1"));
    Assert.assertTrue(states.getDegradedDatacenterNames().isEmpty());
    Assert.assertTrue(states.getDegradedDatacenters().isEmpty());
  }

  @Test
  public void getDegradedDatacentersReturnsUnmodifiable() {
    DegradedDcStates states = new DegradedDcStates();
    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(0, 60, "op"));
    try {
      states.getDegradedDatacenters().put("dc-2", new DegradedDcInfo(0, 60, "op"));
      Assert.fail("getDegradedDatacenters() should return an unmodifiable map");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void getDegradedDatacenterNames() {
    DegradedDcStates states = new DegradedDcStates();
    Assert.assertTrue(states.getDegradedDatacenterNames().isEmpty());

    states.addDegradedDatacenter("dc-1", new DegradedDcInfo(0, 60, "op"));
    states.addDegradedDatacenter("dc-2", new DegradedDcInfo(0, 60, "op"));

    Assert.assertEquals(states.getDegradedDatacenterNames().size(), 2);
    Assert.assertTrue(states.getDegradedDatacenterNames().contains("dc-1"));
    Assert.assertTrue(states.getDegradedDatacenterNames().contains("dc-2"));
  }
}
