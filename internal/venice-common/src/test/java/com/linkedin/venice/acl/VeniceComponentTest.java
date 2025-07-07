package com.linkedin.venice.acl;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VeniceComponentTest {
  @Test
  public void testEnumValues() {
    assertEquals(VeniceComponent.CONTROLLER.getName(), "VeniceController");
    assertEquals(VeniceComponent.ROUTER.getName(), "VeniceRouter");
    assertEquals(VeniceComponent.SERVER.getName(), "VeniceServer");
  }

  @Test
  public void testEnumNames() {
    assertEquals(VeniceComponent.CONTROLLER.name(), "CONTROLLER");
    assertEquals(VeniceComponent.ROUTER.name(), "ROUTER");
    assertEquals(VeniceComponent.SERVER.name(), "SERVER");
  }

  @Test
  public void testEnumOrdinal() {
    assertEquals(VeniceComponent.CONTROLLER.ordinal(), 0);
    assertEquals(VeniceComponent.ROUTER.ordinal(), 1);
    assertEquals(VeniceComponent.SERVER.ordinal(), 2);
  }

  @Test
  public void testEnumValueOf() {
    assertEquals(VeniceComponent.valueOf("CONTROLLER"), VeniceComponent.CONTROLLER);
    assertEquals(VeniceComponent.valueOf("ROUTER"), VeniceComponent.ROUTER);
    assertEquals(VeniceComponent.valueOf("SERVER"), VeniceComponent.SERVER);
  }
}
