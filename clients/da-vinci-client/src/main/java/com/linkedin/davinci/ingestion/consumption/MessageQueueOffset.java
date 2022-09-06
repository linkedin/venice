package com.linkedin.davinci.ingestion.consumption;

import org.apache.commons.lang3.Validate;


/**
 * This class is a POJO containing a message queue (to be consumed) and an offset which refers to a position in this
 * to-be-consumed message queue.
 * @param <MESSAGE_QUEUE> Type of message queue. For example, in the case of Kafka, it is {@link org.apache.kafka.common.TopicPartition}.
 */
public class MessageQueueOffset<MESSAGE_QUEUE> {
  private final MESSAGE_QUEUE messageQueue;
  private final long offset;

  public MessageQueueOffset(MESSAGE_QUEUE messageQueue, long offset) {
    this.messageQueue = Validate.notNull(messageQueue);
    this.offset = offset;
  }

  public MESSAGE_QUEUE getMessageQueue() {
    return messageQueue;
  }

  public long getOffset() {
    return offset;
  }
}
