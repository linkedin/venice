package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_1001_2000;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_151_500;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_2001_5000;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_2_150;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_501_1000;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_EQ_1;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_GT_5000;
import static com.linkedin.venice.stats.dimensions.VeniceRequestKeyCountBucket.KEYS_LE_0;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceRequestKeyCountBucketTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRequestKeyCountBucket, String> expectedValues =
        CollectionUtils.<VeniceRequestKeyCountBucket, String>mapBuilder()
            .put(KEYS_LE_0, "keys_le_0")
            .put(KEYS_EQ_1, "keys_eq_1")
            .put(KEYS_2_150, "keys_2_150")
            .put(KEYS_151_500, "keys_151_500")
            .put(KEYS_501_1000, "keys_501_1000")
            .put(KEYS_1001_2000, "keys_1001_2000")
            .put(KEYS_2001_5000, "keys_2001_5000")
            .put(KEYS_GT_5000, "keys_gt_5000")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceRequestKeyCountBucket.class,
        VeniceMetricsDimensions.VENICE_REQUEST_KEY_COUNT_BUCKET,
        expectedValues).assertAll();
  }

  @DataProvider(name = "boundaries")
  public Object[][] boundaries() {
    return new Object[][] { { Integer.MIN_VALUE, KEYS_LE_0 }, { -1, KEYS_LE_0 }, { 0, KEYS_LE_0 }, { 1, KEYS_EQ_1 },
        { 2, KEYS_2_150 }, { 150, KEYS_2_150 }, { 151, KEYS_151_500 }, { 500, KEYS_151_500 }, { 501, KEYS_501_1000 },
        { 1000, KEYS_501_1000 }, { 1001, KEYS_1001_2000 }, { 2000, KEYS_1001_2000 }, { 2001, KEYS_2001_5000 },
        { 5000, KEYS_2001_5000 }, { 5001, KEYS_GT_5000 }, { Integer.MAX_VALUE, KEYS_GT_5000 } };
  }

  @Test(dataProvider = "boundaries")
  public void testFromKeyCountBoundaries(int keyCount, VeniceRequestKeyCountBucket expected) {
    assertEquals(VeniceRequestKeyCountBucket.fromKeyCount(keyCount), expected);
  }
}
