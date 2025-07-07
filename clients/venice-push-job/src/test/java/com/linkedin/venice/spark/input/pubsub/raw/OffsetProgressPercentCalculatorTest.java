package com.linkedin.venice.spark.input.pubsub.raw;

import com.linkedin.venice.spark.input.pubsub.OffsetProgressPercentCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OffsetProgressPercentCalculatorTest {
  @Test
  public void testSameStartAndEndOffset() {
    // Test case with starting offset of 1 and ending offset of 1
    OffsetProgressPercentCalculator calculator = new OffsetProgressPercentCalculator(1, 1);

    // Test with null offset
    Assert.assertEquals(calculator.calculate(null), 0.0f, "Null offset should return 0.0%");

    // Test with offset less than starting offset
    Assert.assertEquals(calculator.calculate(0L), 0.0f, "Offset less than starting should return 0.0%");

    // Test with offset equal to starting offset
    Assert.assertEquals(calculator.calculate(1L), 100.0f, "Offset equal to starting should return 100.0%");

    // Test with offset greater than ending offset
    Assert.assertEquals(calculator.calculate(2L), 100.0f, "Offset greater than ending should return 100.0%");
  }

  @Test
  public void testConsecutiveOffsets() {
    // Test case with starting offset of 1 and ending offset of 2
    OffsetProgressPercentCalculator calculator = new OffsetProgressPercentCalculator(1, 2);

    // Test with null offset
    Assert.assertEquals(calculator.calculate(null), 0.0f, "Null offset should return 0.0%");

    // Test with offset less than starting offset
    Assert.assertEquals(calculator.calculate(0L), 0.0f, "Offset less than starting should return 0.0%");

    // Test with offset equal to starting offset
    Assert.assertEquals(calculator.calculate(1L), 50.0f, "Offset equal to starting should return 0.0%");

    // Test with offset equal to ending offset
    Assert.assertEquals(calculator.calculate(2L), 100.0f, "Offset equal to ending should return 100.0%");

    // Test with offset greater than ending offset
    Assert.assertEquals(calculator.calculate(3L), 100.0f, "Offset greater than ending should return 100.0%");
  }

  @Test
  public void testAdditionalCases() {
    // Test with a larger range to verify percentage calculation
    OffsetProgressPercentCalculator calculator = new OffsetProgressPercentCalculator(100, 200);

    // Test boundary cases
    Assert.assertEquals(calculator.calculate(100L), 0.0f, "Offset equal to starting should return 0.0%");
    Assert.assertEquals(calculator.calculate(200L), 100.0f, "Offset equal to ending should return 100.0%");

    // Test mid-point
    Assert.assertEquals(calculator.calculate(150L), 50.0f, "Offset at midpoint should return 50.0%");

    // Test 25% and 75% progress
    Assert.assertEquals(calculator.calculate(125L), 25.0f, "Offset at quarter point should return 25.0%");
    Assert.assertEquals(calculator.calculate(175L), 75.0f, "Offset at three-quarter point should return 75.0%");
  }
}
