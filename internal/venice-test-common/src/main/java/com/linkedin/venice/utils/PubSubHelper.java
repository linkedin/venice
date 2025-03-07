package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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

  public static MutableDefaultPubSubMessage getDummyPubSubMessage(boolean isControlMessage) {
    return new MutableDefaultPubSubMessage().setKey(getDummyKey(isControlMessage)).setValue(getDummyValue());
  }

  public static List<MutableDefaultPubSubMessage> produceMessages(
      PubSubProducerAdapter pubSubProducerAdapter,
      PubSubTopicPartition topicPartition,
      int messageCount,
      long delayBetweenMessagesInMs,
      boolean controlMessages) throws InterruptedException, ExecutionException, TimeoutException {
    List<MutableDefaultPubSubMessage> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      MutableDefaultPubSubMessage message = PubSubHelper.getDummyPubSubMessage(controlMessages);
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
              try {
                TimeUnit.MILLISECONDS.sleep(delayBetweenMessagesInMs);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } finally {
                // update the offset and timestamp after produce (and delay) so that each message has a unique timestamp
                message.setOffset(result.getPubSubPosition());
                message.setTimestampAfterProduce(System.currentTimeMillis());
              }
            }
          })
          .get(10, TimeUnit.SECONDS);
      TimeUnit.MILLISECONDS.sleep(delayBetweenMessagesInMs);
    }
    return messages;
  }

  // mutable publish-sub message
  public static class MutableDefaultPubSubMessage implements DefaultPubSubMessage {
    private KafkaKey key;
    private KafkaMessageEnvelope value;
    private PubSubTopicPartition topicPartition;
    private PubSubPosition position;
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

    public PubSubPosition getPosition() {
      return position;
    }

    @Override
    public long getPubSubMessageTime() {
      return 0;
    }

    @Override
    public int getPayloadSize() {
      return 0;
    }

    @Override
    public boolean isEndOfBootstrap() {
      return false;
    }

    public MutableDefaultPubSubMessage setKey(KafkaKey key) {
      this.key = key;
      return this;
    }

    public MutableDefaultPubSubMessage setValue(KafkaMessageEnvelope value) {
      this.value = value;
      return this;
    }

    public MutableDefaultPubSubMessage setTopicPartition(PubSubTopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      return this;
    }

    public MutableDefaultPubSubMessage setOffset(PubSubPosition position) {
      this.position = position;
      return this;
    }

    public MutableDefaultPubSubMessage setTimestampBeforeProduce(long timestampBeforeProduce) {
      this.timestampBeforeProduce = timestampBeforeProduce;
      return this;
    }

    public MutableDefaultPubSubMessage setTimestampAfterProduce(long timestampAfterProduce) {
      this.timestampAfterProduce = timestampAfterProduce;
      return this;
    }

    public long getTimestampBeforeProduce() {
      return timestampBeforeProduce;
    }

    public long getTimestampAfterProduce() {
      return timestampAfterProduce;
    }

    @Override
    public int getHeapSize() {
      throw new UnsupportedOperationException("getHeapSize is not supported on " + this.getClass().getSimpleName());
    }
  }
}
