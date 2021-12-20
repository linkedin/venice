package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerFactory;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DictionaryUtils {
  private static final Logger LOGGER = LogManager.getLogger(DictionaryUtils.class);

  private static Properties getKafkaConsumerProps() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  public static ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    VeniceKafkaConsumerFactory kafkaConsumerFactory = new VeniceKafkaConsumerFactory(props);

    try (KafkaConsumerWrapper kafkaConsumer = kafkaConsumerFactory.getConsumer(getKafkaConsumerProps())) {
      return DictionaryUtils.readDictionaryFromKafka(topicName, kafkaConsumer);
    }
  }

  /**
   * This function reads the kafka topic for the store version for the Start Of Push message which contains the
   * compression dictionary. Once the Start of Push message has been read, the consumer stops.
   * @return The compression dictionary wrapped in a ByteBuffer, or null if no dictionary was present in the
   * Start Of Push message.
   */
  public static ByteBuffer readDictionaryFromKafka(String topicName, KafkaConsumerWrapper kafkaConsumerWrapper) {
    List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(topicName, 0));
    LOGGER.info("Consuming from topic: " + topicName + " till StartOfPush");
    kafkaConsumerWrapper.assign(partitions);
    kafkaConsumerWrapper.seek(partitions.get(0), 0);
    boolean startOfPushReceived = false;
    ByteBuffer compressionDictionary = null;
    while (!startOfPushReceived) {
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = kafkaConsumerWrapper.poll(10 * Time.MS_PER_SECOND);
      for (final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        KafkaKey kafkaKey = record.key();
        KafkaMessageEnvelope kafkaValue = record.value();
        if (kafkaKey.isControlMessage()) {
          ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
          ControlMessageType type = ControlMessageType.valueOf(controlMessage);
          LOGGER.info(
              "Consumed ControlMessage: " + type.name() + " from topic = " + record.topic() + " and partition = " + record.partition());
          if (type == ControlMessageType.START_OF_PUSH) {
            startOfPushReceived = true;
            compressionDictionary = ((StartOfPush) controlMessage.controlMessageUnion).compressionDictionary;
            if (compressionDictionary == null || !compressionDictionary.hasRemaining()) {
              LOGGER.warn(
                  "No dictionary present in Start of Push message from topic = " + record.topic() + " and partition = " + record.partition());
              return null;
            }
            break;
          }
        } else {
          LOGGER.error(
              "Consumed non Control Message before Start of Push from topic = " + record.topic() + " and partition = " + record.partition());
          return null;
        }
      }
    }
    return compressionDictionary;
  }
}
