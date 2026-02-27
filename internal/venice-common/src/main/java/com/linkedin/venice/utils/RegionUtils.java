package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RegionUtils {
  private static final Logger LOGGER = LogManager.getLogger(RegionUtils.class);
  private static final String REGION_FILTER_LIST_SEPARATOR = ",\\s*";
  public static final String UNKNOWN_REGION = "unknown";

  /**
   * Returns the input region name if non-null and non-empty, otherwise {@link #UNKNOWN_REGION}.
   */
  public static String normalizeRegionName(String regionName) {
    return (regionName == null || regionName.isEmpty()) ? UNKNOWN_REGION : regionName;
  }

  public static String getLocalRegionName(VeniceProperties props, boolean isParentRegion) {
    String regionName;
    String regionNameFromConfig = props.getString(LOCAL_REGION_NAME, "");
    if (!StringUtils.isEmpty(regionNameFromConfig)) {
      regionName = regionNameFromConfig + (isParentRegion ? ".parent" : "");
    } else {
      String regionNameFromEnv = null;
      try {
        regionNameFromEnv = System.getenv(ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME);
        LOGGER.info("Region name from environment config: {}", regionNameFromEnv);
        if (regionNameFromEnv == null) {
          regionNameFromEnv = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
          LOGGER.info("Region name from System property: {}", regionNameFromEnv);
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Error when trying to retrieve environment variable for region name; will use default value instead.",
            e);
      }
      regionName = StringUtils.isEmpty(regionNameFromEnv) ? "" : regionNameFromEnv + (isParentRegion ? ".parent" : "");
    }
    return regionName;
  }

  public static String getRegionSpecificMetricPrefix(String localRegionName, String regionName) {
    if (Objects.equals(localRegionName.toLowerCase(), regionName.toLowerCase())) {
      return localRegionName + "_from_" + regionName + "_local";
    } else {
      return localRegionName + "_from_" + regionName + "_remote";
    }
  }

  /**
   * A helper function to split a region list with {@link #REGION_FILTER_LIST_SEPARATOR}
   */
  public static Set<String> parseRegionsFilterList(String regionsFilterList) {
    if (StringUtils.isEmpty(regionsFilterList)) {
      return Collections.emptySet();
    }
    return Stream.of(regionsFilterList.trim().split(REGION_FILTER_LIST_SEPARATOR)).collect(Collectors.toSet());
  }

  /**
   * A helper function to split a region list with {@link #REGION_FILTER_LIST_SEPARATOR}
   */
  public static List<String> parseRegionRolloutOrderList(String rolloutOrderList) {
    if (StringUtils.isEmpty(rolloutOrderList)) {
      return Collections.emptyList();
    }
    return Stream.of(rolloutOrderList.trim().split(REGION_FILTER_LIST_SEPARATOR)).collect(Collectors.toList());
  }

  /**
   * A helper function to check if a region is part of the regions filter list.
   */
  public static boolean isRegionPartOfRegionsFilterList(String region, String regionFilter) {
    if (StringUtils.isEmpty(regionFilter)) {
      return true;
    }
    Set<String> regionsFilter = parseRegionsFilterList(regionFilter);
    return regionsFilter.contains(region);
  }

  /**
   * A helper function to compose a region list with {@link #REGION_FILTER_LIST_SEPARATOR}.
   * This is the reverse of {@link #parseRegionsFilterList(String)}.
   * @param regions, a set of regions
   * @return a string of regions separated by {@link #REGION_FILTER_LIST_SEPARATOR}
   */
  public static String composeRegionList(Set<String> regions) {
    return String.join(",", regions);
  }

  /**
   * Finds the Kafka cluster ID for the local region. Each region has exactly one primary Kafka
   * cluster whose alias matches the region name. Separate RT topic clusters for incremental pushes
   * use a "{region}_sep" alias convention and are treated as distinct regions, so they won't match.
   *
   * @return the local cluster ID, or -1 if not found
   */
  public static int getLocalKafkaClusterId(Int2ObjectMap<String> clusterIdToAlias, String localRegionName) {
    if (localRegionName == null || localRegionName.isEmpty()) {
      return -1;
    }
    for (Int2ObjectMap.Entry<String> entry: clusterIdToAlias.int2ObjectEntrySet()) {
      if (localRegionName.equals(entry.getValue())) {
        return entry.getIntKey();
      }
    }
    return -1;
  }
}
