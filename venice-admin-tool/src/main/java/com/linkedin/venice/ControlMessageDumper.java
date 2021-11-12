package com.linkedin.venice;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


public class ControlMessageDumper {
  private Map<GUID, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> producerToRecords = new HashMap<>();
  private KafkaConsumer consumer;
  private int messageCount;
  private int COUNTDOWN = 3;  // TODO: make this configurable


  public ControlMessageDumper(Properties consumerProps, String topic, int partitionNumber,  int startingOffset, int messageCount) {
    this.messageCount = messageCount;
    this.consumer = new KafkaConsumer(consumerProps);

    TopicPartition partition = new TopicPartition(topic, partitionNumber);
    consumer.assign(Collections.singletonList(partition));
    consumer.seek(partition, startingOffset);
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
      ConsumerRecords records = consumer.poll(1000); // up to 1 second

      Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> iter = records.iterator();
      while (iter.hasNext() && currentMessageCount < messageCount) {
        currentMessageCount++;
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = iter.next();
        KafkaMessageEnvelope envelope = record.value();

        if (MessageType.valueOf(envelope) == MessageType.CONTROL_MESSAGE) {
          GUID producerGUID = envelope.producerMetadata.producerGUID;
          producerToRecords.computeIfAbsent(producerGUID, x -> new LinkedList<>()).add(record);
        }
      }

      System.out.println("Consumed " + currentMessageCount + " messages");
      countdownBeforeStop = records.count() == 0 ? countdownBeforeStop - 1 : COUNTDOWN;
    } while (currentMessageCount < messageCount && countdownBeforeStop > 0);

    return this;
  }


  /**
   *  Display control messages from each producer
   */
  public void display() {
    int i = 1;
    for (GUID producerGUID : producerToRecords.keySet()) {
      System.out.println(String.format("\nproducer %d: %s", i++, producerGUID));

      List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> records = producerToRecords.get(producerGUID);
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        KafkaMessageEnvelope envelope = record.value();
        ProducerMetadata metadata = envelope.producerMetadata;

        if (MessageType.valueOf(envelope) == MessageType.CONTROL_MESSAGE) {
          ControlMessage msg = (ControlMessage) envelope.payloadUnion;
          ControlMessageType msgType = ControlMessageType.valueOf(msg);
          System.out.println();
          System.out.println("offset: " + record.offset());
          System.out.println("segment: " + metadata.segmentNumber);
          System.out.println("sequence number: " + metadata.messageSequenceNumber);
          System.out.println("timestamp1: " + metadata.messageTimestamp);
          System.out.println("timestamp2: " + record.timestamp());
          System.out.println(msgType);

          if (msgType == ControlMessageType.END_OF_SEGMENT) {
            EndOfSegment end = (EndOfSegment) msg.controlMessageUnion;
            System.out.println("is final segment: " + end.finalSegment);
            System.out.println("check sum: " + Arrays.toString(end.checksumValue.array()));
          }
        }
      }
    }
  }
}
