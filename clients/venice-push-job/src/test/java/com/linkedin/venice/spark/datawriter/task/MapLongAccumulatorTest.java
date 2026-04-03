package com.linkedin.venice.spark.datawriter.task;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MapLongAccumulatorTest {
  @Test
  public void testIsZero() {
    MapLongAccumulator accumulator = new MapLongAccumulator();
    Assert.assertTrue(accumulator.isZero());

    accumulator.add(new scala.Tuple2<>(0, 10L));
    Assert.assertFalse(accumulator.isZero());
  }

  @Test
  public void testAddAndValue() {
    MapLongAccumulator accumulator = new MapLongAccumulator();
    accumulator.add(new scala.Tuple2<>(0, 10L));
    accumulator.add(new scala.Tuple2<>(1, 20L));
    accumulator.add(new scala.Tuple2<>(0, 5L));

    Map<Integer, Long> value = accumulator.value();
    Assert.assertEquals(value.size(), 2);
    Assert.assertEquals((long) value.get(0), 15L);
    Assert.assertEquals((long) value.get(1), 20L);
  }

  @Test
  public void testMerge() {
    MapLongAccumulator accA = new MapLongAccumulator();
    accA.add(new scala.Tuple2<>(0, 10L));
    accA.add(new scala.Tuple2<>(1, 20L));

    MapLongAccumulator accB = new MapLongAccumulator();
    accB.add(new scala.Tuple2<>(1, 5L));
    accB.add(new scala.Tuple2<>(2, 30L));

    accA.merge(accB);

    Map<Integer, Long> value = accA.value();
    Assert.assertEquals(value.size(), 3);
    Assert.assertEquals((long) value.get(0), 10L);
    Assert.assertEquals((long) value.get(1), 25L);
    Assert.assertEquals((long) value.get(2), 30L);
  }

  @Test
  public void testReset() {
    MapLongAccumulator accumulator = new MapLongAccumulator();
    accumulator.add(new scala.Tuple2<>(0, 10L));
    accumulator.add(new scala.Tuple2<>(1, 20L));
    Assert.assertFalse(accumulator.isZero());

    accumulator.reset();
    Assert.assertTrue(accumulator.isZero());
    Assert.assertTrue(accumulator.value().isEmpty());
  }

  @Test
  public void testCopy() {
    MapLongAccumulator original = new MapLongAccumulator();
    original.add(new scala.Tuple2<>(0, 10L));
    original.add(new scala.Tuple2<>(1, 20L));

    MapLongAccumulator copy = (MapLongAccumulator) original.copy();

    Assert.assertNotNull(copy);
    Assert.assertEquals(copy.value(), original.value());

    // Verify independence: modifying copy should not affect original
    copy.add(new scala.Tuple2<>(0, 5L));
    Assert.assertEquals((long) original.value().get(0), 10L);
    Assert.assertEquals((long) copy.value().get(0), 15L);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testValueIsUnmodifiable() {
    MapLongAccumulator accumulator = new MapLongAccumulator();
    accumulator.add(new scala.Tuple2<>(0, 10L));
    accumulator.value().put(1, 20L);
  }
}
