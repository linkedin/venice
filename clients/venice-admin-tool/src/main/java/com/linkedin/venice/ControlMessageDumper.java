package com.linkedin.venice;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class ControlMessageDumper {
  private Map<GUID, List<DefaultPubSubMessage>> producerToRecords = new HashMap<>();
  private PubSubConsumerAdapter consumer;
  private int messageCount;
  private int COUNTDOWN = 3; // TODO: make this configurable

  public ControlMessageDumper(
      PubSubConsumerAdapter consumer,
      String topic,
      int partitionNumber,
      int startingOffset,
      int messageCount) {
    this.consumer = consumer;
    this.messageCount = messageCount;
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionNumber);
    consumer.subscribe(partition, startingOffset - 1);
  }

  /**
   * 1. Fetch up to {@link ControlMessageDumper#messageCount} messages in this partition.
   * 2. Discard non-control messages.
   * 3. Split into different groups based on producer
   */
  public ControlMessageDumper fetch() {
    int countdownBeforeStop = COUNTDOWN;
    int currentMessageCount = 0;

    do {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> records = consumer.poll(1000); // up to 1 second
      int recordsCount = records.values().stream().mapToInt(List::size).sum();
      Iterator<DefaultPubSubMessage> recordsIterator = Utils.iterateOnMapOfLists(records);
      while (recordsIterator.hasNext() && currentMessageCount < messageCount) {
        currentMessageCount++;
        DefaultPubSubMessage record = recordsIterator.next();
        KafkaMessageEnvelope envelope = record.getValue();

        if (MessageType.valueOf(envelope) == MessageType.CONTROL_MESSAGE) {
          GUID producerGUID = envelope.producerMetadata.producerGUID;
          producerToRecords.computeIfAbsent(producerGUID, x -> new LinkedList<>()).add(record);
        }
      }

      System.out.println("Consumed " + currentMessageCount + " messages");
      countdownBeforeStop = recordsCount == 0 ? countdownBeforeStop - 1 : COUNTDOWN;
    } while (currentMessageCount < messageCount && countdownBeforeStop > 0);

    return this;
  }

  /**
   *  Display control messages from each producer
   */
  public int display() {
    int i = 1;
    int totalMessages = 0;
    for (Map.Entry<GUID, List<DefaultPubSubMessage>> entry: producerToRecords.entrySet()) {
      GUID producerGUID = entry.getKey();
      System.out.println(String.format("\nproducer %d: %s", i++, producerGUID));

      List<DefaultPubSubMessage> records = entry.getValue();
      totalMessages += records.size();
      for (DefaultPubSubMessage record: records) {
        KafkaMessageEnvelope envelope = record.getValue();
        ProducerMetadata metadata = envelope.producerMetadata;

        if (MessageType.valueOf(envelope) == MessageType.CONTROL_MESSAGE) {
          ControlMessage msg = (ControlMessage) envelope.payloadUnion;
          ControlMessageType msgType = ControlMessageType.valueOf(msg);
          System.out.println();
          System.out.println("offset: " + record.getPosition());
          System.out.println("segment: " + metadata.segmentNumber);
          System.out.println("sequence number: " + metadata.messageSequenceNumber);
          System.out.println("timestamp1: " + metadata.messageTimestamp);
          System.out.println("timestamp2: " + record.getPubSubMessageTime());
          System.out.println(msgType);

          if (msgType == ControlMessageType.END_OF_SEGMENT) {
            EndOfSegment end = (EndOfSegment) msg.controlMessageUnion;
            System.out.println("is final segment: " + end.finalSegment);
            System.out.println("check sum: " + Arrays.toString(end.checksumValue.array()));
          }
        }
      }
    }
    return totalMessages;
  }
}
