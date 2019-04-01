package com.linkedin.venice.controller.kafka.validation;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.validation.ProducerTracker;


/**
 * A {@link ProducerTracker} that is used for tracking DIV info for messages in the admin topic. This class provides
 * some additional methods such as {@code overwriteSequenceNumber} for debugging and fast recovery.
 */
public class AdminProducerTracker extends ProducerTracker {
  public AdminProducerTracker(GUID producerGUID, String topicName) {
    super(producerGUID, topicName);
  }

  public void overwriteSequenceNumber(int partition, int sequenceNumber) {
    segments.get(partition).setSequenceNumber(sequenceNumber);
  }
}
