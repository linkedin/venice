package com.linkedin.venice.participant.protocol.enums;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;


public class PushJobKillTriggerTest {
  @Test
  public void testGetCode() {
    assertEquals(PushJobKillTrigger.USER_REQUEST.getCode(), 0);
    assertEquals(PushJobKillTrigger.VERSION_RETIREMENT.getCode(), 1);
    assertEquals(PushJobKillTrigger.SLA_VIOLATION.getCode(), 2);
    assertEquals(PushJobKillTrigger.PREEMPTED_BY_FULL_PUSH.getCode(), 3);
    assertEquals(PushJobKillTrigger.INGESTION_FAILURE.getCode(), 4);
    assertEquals(PushJobKillTrigger.VERSION_CREATION_FAILURE.getCode(), 5);
    assertEquals(PushJobKillTrigger.PUSH_JOB_FAILED.getCode(), 6);
    assertEquals(PushJobKillTrigger.LINGERING_VERSION_TOPIC.getCode(), 7);
    assertEquals(PushJobKillTrigger.UNKNOWN.getCode(), 8);
  }

  @Test
  public void testFromCode() {
    assertEquals(PushJobKillTrigger.fromCode(0), PushJobKillTrigger.USER_REQUEST);
    assertEquals(PushJobKillTrigger.fromCode(1), PushJobKillTrigger.VERSION_RETIREMENT);
    assertEquals(PushJobKillTrigger.fromCode(2), PushJobKillTrigger.SLA_VIOLATION);
    assertEquals(PushJobKillTrigger.fromCode(3), PushJobKillTrigger.PREEMPTED_BY_FULL_PUSH);
    assertEquals(PushJobKillTrigger.fromCode(4), PushJobKillTrigger.INGESTION_FAILURE);
    assertEquals(PushJobKillTrigger.fromCode(5), PushJobKillTrigger.VERSION_CREATION_FAILURE);
    assertEquals(PushJobKillTrigger.fromCode(6), PushJobKillTrigger.PUSH_JOB_FAILED);
    assertEquals(PushJobKillTrigger.fromCode(7), PushJobKillTrigger.LINGERING_VERSION_TOPIC);
    assertEquals(PushJobKillTrigger.fromCode(8), PushJobKillTrigger.UNKNOWN);
    assertNull(PushJobKillTrigger.fromCode(99));
    assertNull(PushJobKillTrigger.fromCode(-1));
  }

  @Test
  public void testFromString() {
    // Test exact name match (uppercase)
    assertEquals(PushJobKillTrigger.fromString("USER_REQUEST"), PushJobKillTrigger.USER_REQUEST);
    assertEquals(PushJobKillTrigger.fromString("SLA_VIOLATION"), PushJobKillTrigger.SLA_VIOLATION);
    assertEquals(PushJobKillTrigger.fromString("UNKNOWN"), PushJobKillTrigger.UNKNOWN);

    // Test case-insensitive match
    assertEquals(PushJobKillTrigger.fromString("user_request"), PushJobKillTrigger.USER_REQUEST);
    assertEquals(PushJobKillTrigger.fromString("User_Request"), PushJobKillTrigger.USER_REQUEST);
    assertEquals(PushJobKillTrigger.fromString("sla_violation"), PushJobKillTrigger.SLA_VIOLATION);

    // Test null and invalid input
    assertNull(PushJobKillTrigger.fromString(null));
    assertNull(PushJobKillTrigger.fromString(""));
    assertNull(PushJobKillTrigger.fromString("INVALID_TRIGGER"));
  }
}
