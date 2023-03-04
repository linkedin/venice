package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobZstdConfig;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Zstd dict trainer for Kafka Repush.
 * This class will try to read a few records from every partition as samples of dict trainer.
 */
public class KafkaInputDictTrainer {
  public static class Param {
    private String kafkaInputBroker;
    private String topicName;
    private String keySchema;
    private Properties sslProperties;
    private int compressionDictSize;
    private int dictSampleSize;

    Param(ParamBuilder builder) {
      this.kafkaInputBroker = builder.kafkaInputBroker;
      this.topicName = builder.topicName;
      this.keySchema = builder.keySchema;
      this.sslProperties = builder.sslProperties;
      this.compressionDictSize = builder.compressionDictSize;
      this.dictSampleSize = builder.dictSampleSize;
    }
  }

  public static class ParamBuilder {
    private String kafkaInputBroker;
    private String topicName;
    private String keySchema;
    private Properties sslProperties;
    private int compressionDictSize;
    private int dictSampleSize;

    public ParamBuilder setKafkaInputBroker(String kafkaInputBroker) {
      this.kafkaInputBroker = kafkaInputBroker;
      return this;
    }

    public ParamBuilder setTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public ParamBuilder setKeySchema(String keySchema) {
      this.keySchema = keySchema;
      return this;
    }

    public ParamBuilder setSslProperties(Properties sslProperties) {
      this.sslProperties = sslProperties;
      return this;
    }

    public ParamBuilder setCompressionDictSize(int compressionDictSize) {
      this.compressionDictSize = compressionDictSize;
      return this;
    }

    public ParamBuilder setDictSampleSize(int dictSampleSize) {
      this.dictSampleSize = dictSampleSize;
      return this;
    }

    public Param build() {
      return new Param(this);
    }
  }

  private static final Logger LOGGER = LogManager.getLogger(KafkaInputDictTrainer.class);
  private final VeniceProperties props;
  private final JobConf jobConf;
  private final String sourceTopicName;
  private byte[] dict = null;
  private final KafkaInputFormat kafkaInputFormat;
  private final Optional<ZstdDictTrainer> trainerSupplier;

  public KafkaInputDictTrainer(Param param) {
    this(new KafkaInputFormat(), Optional.empty(), param);
  }

  // For testing only
  protected KafkaInputDictTrainer(
      KafkaInputFormat inputFormat,
      Optional<ZstdDictTrainer> trainerSupplier,
      Param param) {
    this.kafkaInputFormat = inputFormat;
    this.trainerSupplier = trainerSupplier;
    Properties properties = new Properties();
    properties.setProperty(KAFKA_INPUT_BROKER_URL, param.kafkaInputBroker);
    properties.setProperty(KAFKA_INPUT_TOPIC, param.topicName);
    properties.setProperty(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, param.keySchema);
    this.sourceTopicName = param.topicName;
    properties.putAll(param.sslProperties);
    properties.setProperty(COMPRESSION_DICTIONARY_SIZE_LIMIT, Integer.toString(param.compressionDictSize));
    properties.setProperty(COMPRESSION_DICTIONARY_SAMPLE_SIZE, Integer.toString(param.dictSampleSize));

    props = new VeniceProperties(properties);
    jobConf = new JobConf();
    properties.forEach((k, v) -> jobConf.set((String) k, (String) v));
  }

  public synchronized byte[] trainDict() {
    if (dict != null) {
      return dict;
    }
    // Prepare input
    // Get one split per partition
    InputSplit[] splits = kafkaInputFormat.getSplitsByRecordsPerSplit(jobConf, Long.MAX_VALUE);
    // Try to gather some records from each partition
    PushJobZstdConfig zstdConfig = new PushJobZstdConfig(props, splits.length);
    ZstdDictTrainer trainer = trainerSupplier.isPresent() ? trainerSupplier.get() : zstdConfig.getZstdDictTrainer();
    int maxBytesPerPartition = zstdConfig.getMaxBytesPerFile();

    KafkaInputMapperKey mapperKey = null;
    KafkaInputMapperValue mapperValue = null;

    int currentPartition = 0;
    long totalSampledRecordCnt = 0;
    try {
      for (InputSplit split: splits) {
        long currentFilledSize = 0;
        long sampledRecordCnt = 0;
        RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> recordReader =
            kafkaInputFormat.getRecordReader(split, jobConf, Reporter.NULL);
        try {
          if (mapperKey == null) {
            mapperKey = recordReader.createKey();
          }
          if (mapperValue == null) {
            mapperValue = recordReader.createValue();
          }
          while (recordReader.next(mapperKey, mapperValue)) {
            byte[] value = ByteUtils.extractByteArray(mapperValue.value);
            currentFilledSize += value.length;
            if (currentFilledSize > maxBytesPerPartition) {
              break;
            }
            trainer.addSample(value);
            ++sampledRecordCnt;
          }
          totalSampledRecordCnt += sampledRecordCnt;
          LOGGER.info("Added {} samples into dict from partition: {}", sampledRecordCnt, currentPartition);
          ++currentPartition;
        } finally {
          recordReader.close();
        }
      }
    } catch (IOException e) {
      throw new VeniceException("Encountered exception while reading source topic: " + sourceTopicName, e);
    }
    if (totalSampledRecordCnt == 0) {
      throw new VeniceException("No record in the source topic: " + sourceTopicName + ", can't train the dict");
    }
    LOGGER.info("Added total {} records from {} partitions into dict", totalSampledRecordCnt, splits.length);
    dict = trainer.trainSamples();
    LOGGER.info("Successfully finished training dict");
    return dict;
  }

}
