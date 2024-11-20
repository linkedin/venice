package com.linkedin.venice.samza;

import org.apache.samza.system.OutgoingMessageEnvelope;


/**
 * This class defines a wrapper object to wrap up the actual object and the corresponding update timestamp.
 *
 * For the users who need to specify the logical update timestamp, you will need to wrap up your update and
 * the corresponding update timestamp and pass it to {@link VeniceSystemProducer}.
 * Specifically, for the users who are using {@link VeniceSystemProducer#send(String, OutgoingMessageEnvelope)},
 * the wrapper will replace {@link OutgoingMessageEnvelope#message}.
 */
public class VeniceObjectWithTimestamp {
  private Object object;
  private long timestamp;

  public VeniceObjectWithTimestamp(Object object, long timestamp) {
    this.object = object;
    this.timestamp = timestamp;
  }

  public Object getObject() {
    return object;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
