package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import java.nio.ByteBuffer;


public class PubSubHelper {
  public static KafkaKey getDummyKey() {
    return new KafkaKey(MessageType.PUT, Utils.getUniqueString("dummyKey").getBytes());
  }

  public static KafkaMessageEnvelope getDummyValue() {
    return getDummyValue(System.currentTimeMillis());
  }

  public static KafkaMessageEnvelope getDummyValue(long producerMessageTimestamp) {
    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.producerMetadata = new ProducerMetadata();
    value.producerMetadata.messageTimestamp = producerMessageTimestamp;
    value.producerMetadata.messageSequenceNumber = 0;
    value.producerMetadata.segmentNumber = 0;
    value.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(1024);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    value.payloadUnion = put;
    return value;
  }
}
