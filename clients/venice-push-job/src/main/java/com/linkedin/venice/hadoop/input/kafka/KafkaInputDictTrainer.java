package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobZstdConfig;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
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
    private CompressionStrategy sourceVersionCompressionStrategy;

    Param(ParamBuilder builder) {
      this.kafkaInputBroker = builder.kafkaInputBroker;
      this.topicName = builder.topicName;
      this.keySchema = builder.keySchema;
      this.sslProperties = builder.sslProperties;
      this.compressionDictSize = builder.compressionDictSize;
      this.dictSampleSize = builder.dictSampleSize;
      this.sourceVersionCompressionStrategy = builder.sourceVersionCompressionStrategy;
    }
  }

  public static class ParamBuilder {
    private String kafkaInputBroker;
    private String topicName;
    private String keySchema;
    private Properties sslProperties;
    private int compressionDictSize;
    private int dictSampleSize;
    private CompressionStrategy sourceVersionCompressionStrategy;

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

    public ParamBuilder setSourceVersionCompressionStrategy(CompressionStrategy compressionStrategy) {
      this.sourceVersionCompressionStrategy = compressionStrategy;
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
  private final CompressionStrategy sourceVersionCompressionStrategy;
  private final CompressorBuilder compressorBuilder;

  public KafkaInputDictTrainer(Param param) {
    this(
        new KafkaInputFormat(),
        Optional.empty(),
        param,
        (compressorFactory, compressionStrategy, kafkaUrl, topic, props) -> KafkaInputUtils
            .getCompressor(compressorFactory, compressionStrategy, kafkaUrl, topic, props));
  }

  interface CompressorBuilder {
    VeniceCompressor getCompressor(
        CompressorFactory compressorFactory,
        CompressionStrategy compressionStrategy,
        String kafkaUrl,
        String topic,
        VeniceProperties props);
  }

  // For testing only
  protected KafkaInputDictTrainer(
      KafkaInputFormat inputFormat,
      Optional<ZstdDictTrainer> trainerSupplier,
      Param param,
      CompressorBuilder compressorBuilder) {
    this.kafkaInputFormat = inputFormat;
    this.trainerSupplier = trainerSupplier;
    this.sourceVersionCompressionStrategy = param.sourceVersionCompressionStrategy;
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

    this.compressorBuilder = compressorBuilder;
  }

  public synchronized byte[] trainDict() {
    if (dict != null) {
      return dict;
    }

    // Prepare input
    // Get one split per partition
    KafkaInputSplit[] splits = (KafkaInputSplit[]) kafkaInputFormat.getSplitsByRecordsPerSplit(jobConf, Long.MAX_VALUE);
    // The following sort is trying to get a deterministic dict with the same input.
    Arrays.sort(splits, Comparator.comparingInt(o -> o.getTopicPartition().partition()));
    // Try to gather some records from each partition
    PushJobZstdConfig zstdConfig = new PushJobZstdConfig(props, splits.length);
    ZstdDictTrainer trainer = trainerSupplier.isPresent() ? trainerSupplier.get() : zstdConfig.getZstdDictTrainer();
    int maxBytesPerPartition = zstdConfig.getMaxBytesPerFile();

    // Get the compressor for source version
    CompressorFactory compressorFactory = new CompressorFactory();
    VeniceCompressor sourceVersionCompressor = compressorBuilder.getCompressor(
        compressorFactory,
        sourceVersionCompressionStrategy,
        jobConf.get(KAFKA_INPUT_BROKER_URL),
        jobConf.get(KAFKA_INPUT_TOPIC),
        props);
    boolean isSourceVersionUsingNoopCompressionStrategy =
        sourceVersionCompressor.getCompressionStrategy().equals(CompressionStrategy.NO_OP);

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
            /**
             * We can only decompress full compressed value here.
             * If the source version is using {@link CompressionStrategy.NO_OP}, the value will be passed to the dict directly.
             * If the source version is using other compression strategies, since we can only decompress the full value here, and the
             * chunked values will be skipped.
             *
             * This logic may have a side effect if only the chunked payloads contain enough materials to build the dict,
             * with this, the dict built won't be very efficient.
             * Since the above is an edge case, and solving it would require a lot of efforts here to
             * assemble the chunks into a full value, we will evaluate this after gaining more experience with this feature.
             */
            byte[] decompressedValue;
            if (isSourceVersionUsingNoopCompressionStrategy) {
              decompressedValue = ByteUtils.extractByteArray(mapperValue.value);
            } else {
              if (mapperValue.schemaId <= 0) {
                // We can't decompress some chunks of a compressed value here.
                continue;
              } else {
                decompressedValue = ByteUtils.extractByteArray(sourceVersionCompressor.decompress(mapperValue.value));
              }
            }
            currentFilledSize += decompressedValue.length;
            if (currentFilledSize > maxBytesPerPartition) {
              break;
            }
            trainer.addSample(decompressedValue);
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
    } finally {
      if (compressorFactory != null) {
        compressorFactory.close();
      }
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
