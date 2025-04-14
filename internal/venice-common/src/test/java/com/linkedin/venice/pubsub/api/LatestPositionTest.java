package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.EarliestPositionTest.DummyPosition;
import java.lang.reflect.Constructor;
import org.testng.annotations.Test;


public class LatestPositionTest {
  @Test
  public void testSingletonInstance() {
    PubSubPosition instance1 = LatestPosition.getInstance();
    PubSubPosition instance2 = PubSubSymbolicPosition.LATEST;

    assertNotNull(instance1, "Singleton instance should not be null");
    assertSame(instance1, instance2, "Should return the same singleton instance");
  }

  @Test
  public void testHeapSizeIsPositive() {
    LatestPosition instance = LatestPosition.getInstance();
    assertTrue(instance.getHeapSize() > 0, "Heap size should be positive");
  }

  @Test
  public void testWireFormat() {
    LatestPosition instance = LatestPosition.getInstance();
    PubSubPositionWireFormat wireFormat = instance.getPositionWireFormat();

    assertNotNull(wireFormat, "Wire format should not be null");
    assertEquals(wireFormat.getType(), -1, "Wire format should have reserved type ID for LATEST");
    assertEquals(wireFormat.getRawBytes().remaining(), 0, "Wire format should have empty byte buffer");
  }

  @Test
  public void testEqualsWithReflectionInstance() throws Exception {
    LatestPosition instance = LatestPosition.getInstance();

    // Create another instance via reflection
    Constructor<LatestPosition> constructor = LatestPosition.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    LatestPosition reflectionInstance = constructor.newInstance();

    assertEquals(reflectionInstance.toString(), "LATEST", "toString should return 'LATEST'");

    assertTrue(instance.equals(reflectionInstance), "Instances should be equal based on class");
    assertTrue(reflectionInstance.equals(instance), "Equality should be symmetric");
    assertEquals(instance.hashCode(), reflectionInstance.hashCode(), "Hash codes should be consistent");
  }

  @Test
  public void testHashCodeConsistency() {
    LatestPosition instance1 = LatestPosition.getInstance();
    LatestPosition instance2 = LatestPosition.getInstance();
    assertEquals(instance1.hashCode(), instance2.hashCode(), "Hash codes should match");
  }

  @Test
  public void testNumericOffset() {
    LatestPosition instance = LatestPosition.getInstance();
    assertEquals(instance.getNumericOffset(), -1L, "Numeric offset should be -1");
  }

  @Test
  public void testComparePositionThrows() {
    LatestPosition instance = LatestPosition.getInstance();
    PubSubPosition dummy = new DummyPosition();

    assertThrows(UnsupportedOperationException.class, () -> instance.comparePosition(dummy));
  }

  @Test
  public void testDiffThrows() {
    LatestPosition instance = LatestPosition.getInstance();
    PubSubPosition dummy = new DummyPosition();

    assertThrows(UnsupportedOperationException.class, () -> instance.diff(dummy));
  }
}
