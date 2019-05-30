package com.linkedin.venice;

import com.linkedin.venice.utils.Time;


public class ConfigConstants {
  /**
   * Start of controller config default value
   */

  /**
   * Default value of sleep interval for polling topic deletion status from ZK.
   */
  public static final int DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS = 2 * Time.MS_PER_SECOND;

  /**
   * End of controller config default value
   */
}
