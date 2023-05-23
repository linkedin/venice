package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;

import java.util.Collections;
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
}
