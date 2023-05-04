package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;


/**
 * Constants for the different types of positions
 * Do not change the values of these constants. They are used to serialize and deserialize the position object.
 * @see PubSubPositionWireFormat
 */
public class PubSubPositionType {
  public static final int APACHE_KAFKA_OFFSET = 0;
}
