package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Constructor;
import org.testng.annotations.Test;


public class EarliestPositionTest {
  @Test
  public void testSingletonInstance() {
    PubSubPosition instance1 = EarliestPosition.getInstance();
    PubSubPosition instance2 = PubSubSymbolicPosition.EARLIEST;

    assertNotNull(instance1);
    assertSame(instance1, instance2, "Should return the same singleton instance");
    assertTrue(instance1.isSymbolic(), "EarliestPosition should be symbolic");
    assertTrue(instance2.isSymbolic(), "EarliestPosition should be symbolic");
  }

  @Test
  public void testHeapSizeIsPositive() {
    EarliestPosition instance = EarliestPosition.getInstance();
    assertTrue(instance.getHeapSize() > 0, "Heap size should be positive");
  }

  @Test
  public void testWireFormat() {
    EarliestPosition instance = EarliestPosition.getInstance();
    PubSubPositionWireFormat wireFormat = instance.getPositionWireFormat();

    assertNotNull(wireFormat);
    assertEquals(wireFormat.getType(), -2, "Wire format should have reserved type ID");
    assertEquals(wireFormat.getRawBytes().remaining(), 0, "Wire format should have empty byte buffer");
  }

  @Test
  public void testEqualsWithReflectionInstance() throws Exception {
    EarliestPosition instance = EarliestPosition.getInstance();

    // Use reflection to create a second instance
    Constructor<EarliestPosition> constructor = EarliestPosition.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    EarliestPosition reflectionInstance = constructor.newInstance();

    // toString
    assertEquals(reflectionInstance.toString(), "EARLIEST");

    // Check equality
    assertEquals(reflectionInstance, instance, "Instances should be equal despite being different objects");
    assertEquals(instance, reflectionInstance, "Equality should be symmetric");

    // Check hashCode consistency
    assertEquals(instance.hashCode(), reflectionInstance.hashCode(), "Hash codes must match for equal instances");
  }

  @Test
  public void testHashCodeConsistency() {
    EarliestPosition instance1 = EarliestPosition.getInstance();
    EarliestPosition instance2 = EarliestPosition.getInstance();

    assertEquals(instance1.hashCode(), instance2.hashCode(), "Hash codes should match");
  }

  @Test
  public void testNumericOffset() {
    EarliestPosition instance = EarliestPosition.getInstance();
    assertEquals(instance.getNumericOffset(), -1, "Numeric offset for EarliestPosition should be -1");
  }
}
