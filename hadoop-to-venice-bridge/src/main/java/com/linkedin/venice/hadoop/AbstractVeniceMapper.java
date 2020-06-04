package com.linkedin.venice.hadoop;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.VeniceAdminToolConsumerFactory;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.hadoop.MapReduceConstants.*;


/**
 * An abstraction of the mapper that would return serialized Avro key/value pairs.
 * The class read config {@link KafkaPushJob#VENICE_MAP_ONLY} to determine whether
 * it will send the output to reducer or Kafka straightly.
 *
 * @param <INPUT_KEY> type of the input key read from InputFormat
 * @param <INPUT_VALUE> type of the input value read from InputFormat
 */

public abstract class AbstractVeniceMapper<INPUT_KEY, INPUT_VALUE>
    implements Mapper<INPUT_KEY, INPUT_VALUE, BytesWritable, BytesWritable> {
  private static final Logger LOGGER = Logger.getLogger(AbstractVeniceMapper.class);

  private boolean isMapperOnly;
  private VeniceReducer reducer = null;
  private VeniceCompressor compressor;

  protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> veniceRecordReader;

  @Override
  public void map(INPUT_KEY inputKey, INPUT_VALUE inputValue, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException {
    byte[] recordKey = veniceRecordReader.getKeyBytes(inputKey, inputValue);
    byte[] recordValue = veniceRecordReader.getValueBytes(inputKey, inputValue);

    if (recordKey == null) {
      throw new VeniceException("Mapper received a empty key record");
    }

    if (recordValue == null) {
      LOGGER.warn("Received null record, skip.");
      if (reporter != null) {
        reporter.incrCounter(COUNTER_GROUP_KAFKA, EMPTY_RECORD, 1);
      }
      return;
    }

    try {
      recordValue = compressor.compress(recordValue);
    } catch (IOException e) {
      throw new VeniceException("Caught an IO exception while trying to to use compression strategy: " +
          compressor.getCompressionStrategy().name(), e);
    }

    if (isMapperOnly) {
      reducer.sendMessageToKafka(recordKey, recordValue, reporter);
      return;
    }

    if (reporter != null) {
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE, recordKey.length);
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE, recordValue.length);
    }

    output.collect(new BytesWritable(recordKey), new BytesWritable(recordValue));
  }

  /**
   * Return a AbstractVeniceRecordReader which will be used to handle all input operations
   */
  AbstractVeniceRecordReader getRecordReader() {
    if (veniceRecordReader == null) {
      LOGGER.warn("venice record reader has not been initialized yet. Please call configure().");
    }

    return veniceRecordReader;
  }

  /**
   * An method for child classes to setup record reader and anything custom. It will be called before
   * {@link #configure(VeniceProperties)}
   */
  abstract protected void configure(VeniceProperties props);

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);

    configure(props);

    if (this.veniceRecordReader == null) {
      throw new VeniceException("Record reader not initialized");
    }

    this.isMapperOnly = props.getBoolean(VENICE_MAP_ONLY);
    if (isMapperOnly) {
      this.reducer = new VeniceReducer(false);
      reducer.configure(job);
    }

    if (CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)) == CompressionStrategy.ZSTD_WITH_DICT) {
      ByteBuffer compressionDictionary = readDictionaryFromKafka(props);
      int compressionLevel = props.getInt(ZSTD_COMPRESSION_LEVEL, Zstd.maxCompressionLevel());

      if (compressionDictionary != null && compressionDictionary.limit() > 0) {
        String topicName = props.getString(TOPIC_PROP);
        this.compressor =
            CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, topicName, compressionDictionary.array(), compressionLevel);
      }
    } else {
      this.compressor =
          CompressorFactory.getCompressor(CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)));
    }
  }

  private Properties getKafkaConsumerProps() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }


  /**
   * This function reads the kafka topic for the store version for the Start Of Push message which contains the
   * compression dictionary. Once the Start of Push message has been read, the consumer stops.
   * @return The compression dictionary wrapped in a ByteBuffer, or null if no dictionary was present in the
   * Start Of Push message.
   */
  private ByteBuffer readDictionaryFromKafka(VeniceProperties props) {
    KafkaClientFactory kafkaClientFactory = new VeniceAdminToolConsumerFactory(props);

    try (KafkaConsumer<byte[], byte[]> consumer = kafkaClientFactory.getKafkaConsumer(getKafkaConsumerProps())) {
      String topicName = props.getString(TOPIC_PROP);
      List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(topicName, 0));
      LOGGER.info("Consuming from topic: " + topicName + " till StartOfPush");
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      while (true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10 * Time.MS_PER_SECOND);
        for (final ConsumerRecord<byte[], byte[]> record : records) {
          KafkaKeySerializer keySerializer = new KafkaKeySerializer();
          KafkaKey kafkaKey = keySerializer.deserialize(topicName, record.key());

          KafkaValueSerializer valueSerializer = new KafkaValueSerializer();
          KafkaMessageEnvelope kafkaValue = valueSerializer.deserialize(topicName, record.value());
          if (kafkaKey.isControlMessage()) {
            ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
            ControlMessageType type = ControlMessageType.valueOf(controlMessage);
            LOGGER.info(
                "Consumed ControlMessage: " + type.name() + " from topic = " + record.topic() + " and partition = " + record.partition());
            if (type == ControlMessageType.START_OF_PUSH) {
              ByteBuffer compressionDictionary = ((StartOfPush) controlMessage.controlMessageUnion).compressionDictionary;
              if (compressionDictionary == null || !compressionDictionary.hasRemaining()) {
                LOGGER.warn(
                    "No dictionary present in Start of Push message from topic = " + record.topic() + " and partition = " + record.partition());
                return null;
              }
              return compressionDictionary;
            }
          } else {
            LOGGER.error(
                "Consumed non Control Message before Start of Push from topic = " + record.topic() + " and partition = " + record.partition());
            return null;
          }
        }
      }
    }
  }
}