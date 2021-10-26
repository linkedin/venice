package com.linkedin.venice.utils;

import java.util.Objects;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;


public class RegionUtils {
  public static final Logger logger = Logger.getLogger(RegionUtils.class.getName());

  public static String getLocalRegionName(VeniceProperties props, boolean isParentRegion) {
    String regionName;
    String regionNameFromConfig = props.getString(LOCAL_REGION_NAME, "");
    if (!Utils.isNullOrEmpty(regionNameFromConfig)) {
      regionName = regionNameFromConfig + (isParentRegion ? ".parent" : "");
    } else {
      String regionNameFromEnv = null;
      try {
        regionNameFromEnv = System.getenv(ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME);
        logger.info("Region name from environment config: " + regionNameFromEnv);
        if (regionNameFromEnv == null) {
          regionNameFromEnv = System.getProperty(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION);
          logger.info("Region name from System property: " + regionNameFromEnv);
        }
      } catch (Exception e) {
        logger.warn("Error when trying to retrieve environment variable for region name; will use default value instead.", e);
      }
      regionName = Utils.isNullOrEmpty(regionNameFromEnv)
          ? "" : regionNameFromEnv + (isParentRegion ? ".parent" : "");
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
}
