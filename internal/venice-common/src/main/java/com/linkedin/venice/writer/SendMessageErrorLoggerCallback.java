package com.linkedin.venice.writer;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import org.apache.logging.log4j.Logger;


class SendMessageErrorLoggerCallback implements PubSubProducerCallback, Measurable {
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(SendMessageErrorLoggerCallback.class);
  private final KafkaMessageEnvelope value;
  private final Logger logger;

  public SendMessageErrorLoggerCallback(KafkaMessageEnvelope value, Logger logger) {
    this.value = value;
    this.logger = logger;
  }

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception e) {
    if (e != null) {
      logger.error(
          "Failed to send out message to Kafka producer: [value.messageType: {}, value.producerMetadata: {}]",
          value.messageType,
          value.producerMetadata,
          e);
    }
  }

  /**
   * N.B.: For this use case, the shallow overhead is expected to be the relevant size, since both of the fields should
   * point to shared instances.
   */
  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }
}
