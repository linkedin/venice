package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.spark.SparkConstants;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MaxAccumulatorTest {
  private SparkSession spark;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder().appName("TestApp").master(SparkConstants.DEFAULT_SPARK_CLUSTER).getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testResetAndIsZero() {
    MaxAccumulator<Integer> accumulator = new MaxAccumulator<>(100, spark.sparkContext(), "Test accumulator");

    Assert.assertFalse(accumulator.isZero());
    accumulator.reset();
    Assert.assertTrue(accumulator.isZero());
  }

  @Test
  public void testCopy() {
    MaxAccumulator<Integer> originalAccumulator = new MaxAccumulator<>(100, spark.sparkContext(), "Test accumulator");

    MaxAccumulator<Integer> copyAccumulator = (MaxAccumulator<Integer>) originalAccumulator.copy();

    Assert.assertNotNull(copyAccumulator);
    Assert.assertNotEquals(copyAccumulator, originalAccumulator);
    Assert.assertEquals(copyAccumulator.value(), originalAccumulator.value());

    copyAccumulator.add(200);
    Assert.assertNotEquals(copyAccumulator.value(), originalAccumulator.value());
  }

  @Test
  public void testAdd() {
    MaxAccumulator<Integer> accumulator = new MaxAccumulator<>(100, spark.sparkContext(), "Test accumulator");

    Assert.assertEquals(accumulator.value(), Integer.valueOf(100));

    // No Change
    accumulator.add(50);
    Assert.assertEquals(accumulator.value(), Integer.valueOf(100));

    // Change
    accumulator.add(200);
    Assert.assertEquals(accumulator.value(), Integer.valueOf(200));
  }

  @Test
  public void testMerge() {
    MaxAccumulator<Integer> accumulatorA = new MaxAccumulator<>(100, spark.sparkContext(), "Test accumulator A");
    MaxAccumulator<Integer> accumulatorB = new MaxAccumulator<>(200, spark.sparkContext(), "Test accumulator B");

    Assert.assertEquals(accumulatorA.value(), Integer.valueOf(100));

    accumulatorA.merge(accumulatorB);

    Assert.assertEquals(accumulatorA.value(), Integer.valueOf(200));
  }
}
