package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Generic interface for providing GUIDs.
 *
 * Visibility is restricted to package-private on purpose.
 *
 * @see GuidUtils#getGUID(VeniceProperties)
 */

interface GuidGenerator {
  /**
   * Generates a guid.
   * @return a {@link GUID}
   */
  GUID getGuid();
}
