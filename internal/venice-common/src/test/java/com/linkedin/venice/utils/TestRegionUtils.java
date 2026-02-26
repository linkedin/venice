package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;
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
    Assert.assertEquals(RegionUtils.composeRegionList(regionName), single);

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

  @Test
  public void testParseRolloutOrderList() {
    String rolloutOrder = String.join(",", TEST_DC_1, TEST_DC_2, TEST_DC_3);
    List<String> order = RegionUtils.parseRegionRolloutOrderList(rolloutOrder);
    Assert.assertEquals(order.get(0), TEST_DC_1);
    Assert.assertEquals(order.get(1), TEST_DC_2);
    Assert.assertEquals(order.get(2), TEST_DC_3);
  }

  @Test
  public void testNormalizeRegionName() {
    Assert.assertEquals(RegionUtils.normalizeRegionName("dc-0"), "dc-0");
    Assert.assertEquals(RegionUtils.normalizeRegionName(null), RegionUtils.UNKNOWN_REGION);
    Assert.assertEquals(RegionUtils.normalizeRegionName(""), RegionUtils.UNKNOWN_REGION);
    Assert.assertEquals(RegionUtils.normalizeRegionName(RegionUtils.UNKNOWN_REGION), RegionUtils.UNKNOWN_REGION);
  }

  @Test
  public void testIsRegionPartOfRegionsFilterList() {
    String regionFilter = "dc-0,dc-1,dc-2";

    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_1, regionFilter));
    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_2, regionFilter));
    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_3, regionFilter));

    Assert.assertFalse(RegionUtils.isRegionPartOfRegionsFilterList("dc-3", regionFilter));

    // Test with empty filter list
    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_1, ""));
    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_2, ""));
    Assert.assertTrue(RegionUtils.isRegionPartOfRegionsFilterList(TEST_DC_3, ""));
  }

  @Test
  public void testGetLocalKafkaClusterId() {
    Int2ObjectMap<String> clusterIdToAlias = new Int2ObjectOpenHashMap<>();
    clusterIdToAlias.put(0, TEST_DC_1);
    clusterIdToAlias.put(1, TEST_DC_2);
    clusterIdToAlias.put(2, TEST_DC_3);

    // Match found
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, TEST_DC_1), 0);
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, TEST_DC_2), 1);
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, TEST_DC_3), 2);

    // No match
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, "dc-99"), -1);

    // Separate RT topic cluster (dc-1_sep) should not match dc-1
    clusterIdToAlias.put(3, "dc-1_sep");
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, TEST_DC_2), 1);

    // Null or empty region name
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, null), -1);
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(clusterIdToAlias, ""), -1);

    // Empty map
    Int2ObjectMap<String> emptyMap = new Int2ObjectOpenHashMap<>();
    Assert.assertEquals(RegionUtils.getLocalKafkaClusterId(emptyMap, TEST_DC_1), -1);
  }
}
