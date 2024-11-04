package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;


public class ProtocolUtils {
  /**
   * Measure the heap usage of {@link KafkaMessageEnvelope}.
   */
  public static int getEstimateOfMessageEnvelopeSizeOnHeap(KafkaMessageEnvelope messageEnvelope) {
    int kmeBaseOverhead = 100; // Super rough estimate. TODO: Measure with a more precise library and store statically
    switch (MessageType.valueOf(messageEnvelope)) {
      case PUT:
        Put put = (Put) messageEnvelope.payloadUnion;
        int size = put.putValue.capacity();
        if (put.replicationMetadataPayload != null
            /**
             * N.B.: When using the {@link org.apache.avro.io.OptimizedBinaryDecoder}, the {@link put.putValue} and the
             *       {@link put.replicationMetadataPayload} will be backed by the same underlying array. If that is the
             *       case, then we don't want to account for the capacity twice.
             */
            && put.replicationMetadataPayload.array() != put.putValue.array()) {
          size += put.replicationMetadataPayload.capacity();
        }
        return size + kmeBaseOverhead;
      case UPDATE:
        Update update = (Update) messageEnvelope.payloadUnion;
        return update.updateValue.capacity() + kmeBaseOverhead;
      default:
        return kmeBaseOverhead;
    }
  }
}
