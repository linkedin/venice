package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.Logger;


class KafkaMessageCallback implements Callback {
  private final KafkaMessageEnvelope value;
  private final Logger logger;

  public KafkaMessageCallback(KafkaMessageEnvelope value, Logger logger) {
    this.value = value;
    this.logger = logger;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    if (e != null) {
      logger.error(
          "Failed to send out message to Kafka producer: [value.messageType: " + value.messageType
              + ", value.producerMetadata: " + value.producerMetadata + "]",
          e);
    }
  }
}
