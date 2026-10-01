package com.linkedin.venice.utils;

import static org.testng.Assert.*;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.TreeMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class BatchGetConfigUtilsTest {
  @DataProvider
  public Object[][] invalidServerRanges() {
    return new Object[][] { { "" }, { " " }, { "1-:0" }, { "1-:-1" }, { "0-:8" }, { "-1-:8" }, { "2-:8" },
        { "1-:2147484" }, { "1-:2147483648" }, { "1-2147483648:8" }, { "1-:8," }, { "1-:8:9" }, { "1--:8" },
        { "1-10:8,12-:9" }, { "1-10:8,10-:9" }, { "1-:8,1-:9" }, { "1-2:8,3-2:9,3-:10" }, { "1-2147483647:8" },
        { "1-:8,2147483647-:9" }, { "1-10:8" }, { "1:8" } };
  }

  @Test(dataProvider = "invalidServerRanges", expectedExceptions = VeniceException.class)
  public void testInvalidServerPolicy(String ranges) {
    MultiKeyLongTailRetryPolicy.parse(ranges);
  }

  @Test
  public void testServerPolicyBoundariesAndCheckedConversion() {
    MultiKeyLongTailRetryPolicy policy =
        MultiKeyLongTailRetryPolicy.parse("501-:500,13-20:30,1-12:8,21-150:50,151-500:100");
    int[][] cases = { { 1, 8000 }, { 12, 8000 }, { 13, 30000 }, { 20, 30000 }, { 21, 50000 }, { 150, 50000 },
        { 151, 100000 }, { 500, 100000 }, { 501, 500000 }, { 4999, 500000 }, { 5000, 500000 }, { 5001, 500000 },
        { Integer.MAX_VALUE, 500000 } };
    for (int[] testCase: cases) {
      assertEquals(policy.getRetryThresholdInMicroSeconds(testCase[0]), testCase[1]);
    }
    assertEquals(MultiKeyLongTailRetryPolicy.parse("1-:2147483").getRetryThresholdInMicroSeconds(1), 2147483000);
    assertEquals(
        MultiKeyLongTailRetryPolicy.parse("1-2147483646:1,2147483647-:2")
            .getRetryThresholdInMicroSeconds(Integer.MAX_VALUE),
        2000);
    assertThrows(IllegalArgumentException.class, () -> policy.getRetryThresholdInMicroSeconds(0));
    // Legacy local semantics are intentionally untouched.
    assertEquals((int) BatchGetConfigUtils.parseRetryThresholdForBatchGet("1-:0").get(1), 0);
  }

  @Test
  public void testParseRetryThresholdForBatchGet() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200:80,201-:1000";
    TreeMap<Integer, Integer> retryThresholdMap =
        BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
    assertEquals((int) retryThresholdMap.get(1), 20);
    assertEquals((int) retryThresholdMap.get(11), 50);
    assertEquals((int) retryThresholdMap.get(51), 80);
    assertEquals((int) retryThresholdMap.get(201), 1000);

    assertEquals((int) retryThresholdMap.floorEntry(1).getValue(), 20);
    assertEquals((int) retryThresholdMap.floorEntry(30).getValue(), 50);
    assertEquals((int) retryThresholdMap.floorEntry(500).getValue(), 1000);

    // Config with un-ordered range
    String unorderedRetryThresholdConfig = "51-200:80,11-50:50,201-:1000,1-10:20";
    retryThresholdMap = BatchGetConfigUtils.parseRetryThresholdForBatchGet(unorderedRetryThresholdConfig);
    assertEquals((int) retryThresholdMap.get(1), 20);
    assertEquals((int) retryThresholdMap.get(11), 50);
    assertEquals((int) retryThresholdMap.get(51), 80);
    assertEquals((int) retryThresholdMap.get(201), 1000);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithKeyRangeGap() {
    String retryThresholdConfig = "1-10:20,51-200:80,201-:1000";
    BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithWithInvalidFormat() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-:80,201-:1000";
    BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithInvalidSeparator() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200::80,201-:1000";
    BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutStartingFrom1() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-:1000";
    BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutUnlimitedKeyCount() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-500:1000";
    BatchGetConfigUtils.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }
}
