package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaRecordReader.java
 *
 * This class is used to read data off a Kafka topic partition.
 * It will return the key bytes as unchanged, and extract the following fields and wrap them
 * up as {@link KafkaInputMapperValue} as the value:
 * 1. Value bytes.
 * 2. Schema Id.
 * 3. Offset.
 * 4. Value type, which could be 'PUT' or 'DELETE'.
 */
public class KafkaInputRecordReader implements RecordReader<BytesWritable, KafkaInputMapperValue> {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

  private static final Logger LOGGER = Logger.getLogger(KafkaInputRecordReader.class);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second
  /**
   * Retry when the poll is returning empty result.
   */
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);

  private final KafkaConsumerWrapper consumer;
  private final TopicPartition topicPartition;
  private long maxNumberOfRecords;
  private long startingOffset;
  private long currentOffset;
  private long endingOffset;
  /**
   * Iterator pointing to the current messages fetched from the Kafka topic partition.
   */
  private Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordIterator;


  public KafkaInputRecordReader(InputSplit split, JobConf job, Reporter reporter) {
    if (!(split instanceof KafkaInputSplit)) {
      throw new VeniceException("InputSplit for RecordReader is not valid split type.");
    }
    KafkaInputSplit inputSplit = (KafkaInputSplit)split;
    KafkaClientFactory kafkaClientFactory = KafkaInputUtils.getConsumerFactory(job);
    this.consumer = kafkaClientFactory.getConsumer(KafkaInputUtils.getConsumerProperties());
    this.topicPartition = inputSplit.getTopicPartition();
    this.startingOffset = inputSplit.getStartingOffset();
    this.currentOffset = inputSplit.getStartingOffset() - 1;
    this.endingOffset = inputSplit.getEndingOffset();
    /**
     * Not accurate since the topic partition could be log compacted.
     */
    this.maxNumberOfRecords = endingOffset - startingOffset;
    this.consumer.subscribe(topicPartition.topic(), topicPartition.partition(), currentOffset);
  }

  /**
   * This function will skip all the Control Messages right now.
   */
  @Override
  public boolean next(BytesWritable key, KafkaInputMapperValue value) throws IOException {
    while (hasPendingData()) {
      try {
        loadRecords();
      } catch (InterruptedException e) {
        throw new IOException("Got interrupted while loading records from topic partition: " + topicPartition + " with current offset: " + currentOffset);
      }
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = recordIterator.hasNext() ? recordIterator.next() : null;
      if (record != null) {
        currentOffset = record.offset();

        KafkaKey kafkaKey = record.key();
        KafkaMessageEnvelope kafkaMessageEnvelope = record.value();

        if (kafkaKey.isControlMessage()) {
          // Skip all the control messages
          continue;
        }
        key.set(kafkaKey.getKey(), 0, kafkaKey.getKeyLength());

        value.offset = record.offset();
        MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);
        switch (messageType) {
          case PUT:
            Put put = (Put)kafkaMessageEnvelope.payloadUnion;
            value.valueType = MapperValueType.PUT;
            value.value = put.putValue;
            value.schemaId = put.schemaId;
            break;
          case DELETE:
            value.valueType = MapperValueType.DELETE;
            value.value = EMPTY_BYTE_BUFFER;
            value.schemaId = -1;
            break;
          default:
            throw new IOException("Unexpected '" + messageType + "' message from Kafka topic partition: " + topicPartition + " with offset: " + record.offset());
        }

        return true;
      } else {
        // We have pending data but we are unable to fetch any records so throw an exception and stop the job
        throw new IOException("Unable to read additional data from Kafka. See logs for details. Partition " +
            topicPartition + " Current Offset: " + currentOffset + " End Offset: " + endingOffset);
      }
    }
    return false;
  }

  @Override
  public BytesWritable createKey() {
    return new BytesWritable();
  }

  @Override
  public KafkaInputMapperValue createValue() {
    return new KafkaInputMapperValue();
  }

  @Override
  public long getPos() throws IOException {
    return currentOffset;
  }

  @Override
  public void close() throws IOException {
    this.consumer.close();
  }

  @Override
  public float getProgress() throws IOException {
    //not most accurate but gives reasonable estimate
    return ((float) (currentOffset - startingOffset + 1)) / maxNumberOfRecords;
  }

  private boolean hasPendingData() {
    /**
     * Offset range is exclusive at the end which means the ending offset is one higher
     * than the actual physical last offset
     */
    return currentOffset < endingOffset - 1;
  }

  /**
   * Loads new records into the record iterator
   */
  private void loadRecords() throws InterruptedException {
    if ((recordIterator == null) || !recordIterator.hasNext()) {
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = null;
      int retry = 0;
      while (retry++ < CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES) {
        records = consumer.poll(CONSUMER_POLL_TIMEOUT);
        if (!records.isEmpty()) {
          break;
        }
        Thread.sleep(EMPTY_POLL_SLEEP_TIME_MS);
      }
      if (records.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Consumer#poll still returns empty result after retrying ")
            .append(CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES).append(" times, ")
            .append("topic partition: ").append(topicPartition)
            .append(" and current offset: ").append(currentOffset);
        throw new VeniceException(sb.toString());
      }

      recordIterator = records.iterator();
    }
  }
}
