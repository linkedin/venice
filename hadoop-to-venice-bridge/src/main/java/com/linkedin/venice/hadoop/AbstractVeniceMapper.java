package com.linkedin.venice.hadoop;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
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

    if (reporter != null) {
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE, recordValue.length);
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
    VeniceProperties props;

    SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(job.get(SSL_CONFIGURATOR_CLASS_CONFIG));
    try {
      Properties javaProps = configurator.setupSSLConfig(HadoopUtils.getProps(job), UserCredentialsFactory.getHadoopUserCredentials());
      props = new VeniceProperties(javaProps);
    } catch (IOException e) {
      throw new VeniceException("Could not get user credential for job:" + job.getJobName(), e);
    }

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
      String topicName = props.getString(TOPIC_PROP);
      ByteBuffer compressionDictionary = DictionaryUtils.readDictionaryFromKafka(topicName, props);
      int compressionLevel = props.getInt(ZSTD_COMPRESSION_LEVEL, Zstd.maxCompressionLevel());

      if (compressionDictionary != null && compressionDictionary.limit() > 0) {
        this.compressor =
            CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, topicName, compressionDictionary.array(), compressionLevel);
      }
    } else {
      this.compressor =
          CompressorFactory.getCompressor(CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)));
    }
  }
}