package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class PubSubHelper {
  public static KafkaKey getDummyKey() {
    return getDummyKey(false);
  }

  public static KafkaKey getDummyKey(boolean isControlMessage) {
    if (isControlMessage) {
      return new KafkaKey(MessageType.CONTROL_MESSAGE, Utils.getUniqueString("dummyCmKey").getBytes());
    }
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

  public static MutablePubSubMessage getDummyPubSubMessage(boolean isControlMessage) {
    return new MutablePubSubMessage().setKey(getDummyKey(isControlMessage)).setValue(getDummyValue());
  }

  public static List<MutablePubSubMessage> produceMessages(
      PubSubProducerAdapter pubSubProducerAdapter,
      PubSubTopicPartition topicPartition,
      int messageCount,
      long delayBetweenMessagesInMs,
      boolean controlMessages) throws InterruptedException, ExecutionException, TimeoutException {
    List<MutablePubSubMessage> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      MutablePubSubMessage message = PubSubHelper.getDummyPubSubMessage(controlMessages);
      message.getValue().getProducerMetadata().setMessageTimestamp(i); // logical ts
      message.setTimestampBeforeProduce(System.currentTimeMillis());
      messages.add(message);
      pubSubProducerAdapter
          .sendMessage(
              topicPartition.getTopicName(),
              topicPartition.getPartitionNumber(),
              message.getKey(),
              message.getValue(),
              null,
              null)
          .whenComplete((result, throwable) -> {
            if (throwable == null) {
              message.setOffset(result.getOffset());
              message.setTimestampAfterProduce(System.currentTimeMillis());
              try {
                TimeUnit.MILLISECONDS.sleep(delayBetweenMessagesInMs);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          })
          .get(10, TimeUnit.SECONDS);
      TimeUnit.MILLISECONDS.sleep(delayBetweenMessagesInMs);
    }
    return messages;
  }

  // mutable publish-sub message
  public static class MutablePubSubMessage {
    private KafkaKey key;
    private KafkaMessageEnvelope value;
    private PubSubTopicPartition topicPartition;
    private long offset;
    private long timestampBeforeProduce;
    private long timestampAfterProduce;

    public KafkaKey getKey() {
      return key;
    }

    public KafkaMessageEnvelope getValue() {
      return value;
    }

    public PubSubTopicPartition getTopicPartition() {
      return topicPartition;
    }

    public Long getOffset() {
      return offset;
    }

    public MutablePubSubMessage setKey(KafkaKey key) {
      this.key = key;
      return this;
    }

    public MutablePubSubMessage setValue(KafkaMessageEnvelope value) {
      this.value = value;
      return this;
    }

    public MutablePubSubMessage setTopicPartition(PubSubTopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      return this;
    }

    public MutablePubSubMessage setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public MutablePubSubMessage setTimestampBeforeProduce(long timestampBeforeProduce) {
      this.timestampBeforeProduce = timestampBeforeProduce;
      return this;
    }

    public MutablePubSubMessage setTimestampAfterProduce(long timestampAfterProduce) {
      this.timestampAfterProduce = timestampAfterProduce;
      return this;
    }

    public long getTimestampBeforeProduce() {
      return timestampBeforeProduce;
    }

    public long getTimestampAfterProduce() {
      return timestampAfterProduce;
    }
  }
}
