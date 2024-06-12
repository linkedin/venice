package com.linkedin.venice;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ConfigConstants {
  private static final Logger LOGGER = LogManager.getLogger(ConfigConstants.class);
  /**
   * Start of controller config default value
   */

  public static final int UNSPECIFIED_REPLICATION_METADATA_VERSION = -1;

  public static final long DEFAULT_PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS =
      TimeUnit.MINUTES.toSeconds(10);

  /**
   * End of controller config default value
   */
}
