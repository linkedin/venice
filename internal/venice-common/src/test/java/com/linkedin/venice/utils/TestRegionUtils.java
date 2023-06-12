package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import java.util.Properties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRegionUtils {
  private final static String TEST_DC_1 = "dc-0";
  private final static String TEST_DC_2 = "dc-1";
  private final static String TEST_DC_3 = "dc-2";

  @Test
  public void testGetLocalRegionName() {
    Properties prop = new Properties();
    prop.setProperty(LOCAL_REGION_NAME, TEST_DC_1);
    VeniceProperties props = new VeniceProperties(prop);
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, false), TEST_DC_1);
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, true), TEST_DC_1 + ".parent");

    prop.remove(LOCAL_REGION_NAME);
    props = new VeniceProperties(prop);
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, false), "");
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, true), "");

    System.setProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION, TEST_DC_2);
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, false), TEST_DC_2);
    Assert.assertEquals(RegionUtils.getLocalRegionName(props, true), TEST_DC_2 + ".parent");
  }

  @Test
  public void testGetRegionSpecificMetricPrefix() {
    Assert.assertEquals(RegionUtils.getRegionSpecificMetricPrefix(TEST_DC_1, TEST_DC_1), "dc-0_from_dc-0_local");
    Assert.assertEquals(RegionUtils.getRegionSpecificMetricPrefix(TEST_DC_1, TEST_DC_2), "dc-0_from_dc-1_remote");
  }

  @Test
  public void testParseRegionsFilterList() {
    String single = "dc-0";
    Set<String> regionName = RegionUtils.parseRegionsFilterList(single);
    Assert.assertTrue(regionName.contains(TEST_DC_1));
    Assert.assertEquals(regionName.size(), 1);

    String multiple = "dc-0,dc-1";
    regionName = RegionUtils.parseRegionsFilterList(multiple);
    Assert.assertTrue(regionName.contains(TEST_DC_1));
    Assert.assertTrue(regionName.contains(TEST_DC_2));
    Assert.assertEquals(regionName.size(), 2);

    multiple = "dc-0,dc-1,     dc-2";
    regionName = RegionUtils.parseRegionsFilterList(multiple);
    Assert.assertTrue(regionName.contains(TEST_DC_1));
    Assert.assertTrue(regionName.contains(TEST_DC_2));
    Assert.assertTrue(regionName.contains(TEST_DC_3));
    Assert.assertEquals(regionName.size(), 3);
  }
}
