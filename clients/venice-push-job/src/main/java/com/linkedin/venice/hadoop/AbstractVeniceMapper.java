package com.linkedin.venice.hadoop;

import static com.linkedin.venice.compression.CompressionStrategy.NO_OP;
import static com.linkedin.venice.compression.CompressionStrategy.ZSTD_WITH_DICT;
import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_STRATEGY;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.ZSTD_COMPRESSION_LEVEL;
import static com.linkedin.venice.hadoop.VenicePushJob.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static com.linkedin.venice.hadoop.VenicePushJob.ZSTD_DICTIONARY_CREATION_SUCCESS;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstraction of the mapper that would return serialized, and potentially
 * compressed, Avro key/value pairs.
 *
 * @param <INPUT_KEY> type of the input key read from InputFormat
 * @param <INPUT_VALUE> type of the input value read from InputFormat
 */

public abstract class AbstractVeniceMapper<INPUT_KEY, INPUT_VALUE> extends AbstractMapReduceTask
    implements Mapper<INPUT_KEY, INPUT_VALUE, BytesWritable, BytesWritable> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractVeniceMapper.class);
  private static final int TASK_ID_WHICH_SHOULD_SPRAY_ALL_PARTITIONS = 0;

  // Compression related
  private CompressionStrategy compressionStrategy;
  private boolean compressionMetricCollectionEnabled;
  private boolean isZstdDictCreationSuccess;
  private boolean isZstdDictCreationRequired;
  private CompressorFactory compressorFactory;
  private VeniceCompressor[] compressor;

  byte[] recordKey = null, recordValue = null;
  BytesWritable keyBW = new BytesWritable(), valueBW = new BytesWritable();

  protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> veniceRecordReader;
  protected FilterChain<INPUT_VALUE> veniceFilterChain;

  @Override
  public void map(
      INPUT_KEY inputKey,
      INPUT_VALUE inputValue,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) throws IOException {
    if (recordKey == null) {
      maybeSprayAllPartitions(output, reporter);
    }
    if (process(inputKey, inputValue, keyBW, valueBW, reporter)) {
      // key/value pair is valid.
      output.collect(keyBW, valueBW);
    }
  }

  private void maybeSprayAllPartitions(OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException {
    /** First map invocation, since the {@link recordKey} will be set after this. */
    if (TASK_ID_NOT_SET == getTaskId()) {
      throw new IllegalStateException("attemptID not set!");
    }
    if (TASK_ID_WHICH_SHOULD_SPRAY_ALL_PARTITIONS != getTaskId()) {
      return;
    }
    keyBW.setSize(VeniceMRPartitioner.EMPTY_KEY_LENGTH);
    recordValue = new byte[Integer.BYTES];
    for (int i = 0; i < getPartitionCount(); i++) {
      ByteUtils.writeInt(recordValue, i, 0);
      valueBW.set(recordValue, 0, Integer.BYTES);
      output.collect(keyBW, valueBW);
    }
    MRJobCounterHelper.incrMapperSprayAllPartitionsTriggeredCount(reporter, 1);
    LOGGER.info(
        "Map Task ID {} successfully sprayed all partitions, to ensure that all Reducers come up.",
        TASK_ID_WHICH_SHOULD_SPRAY_ALL_PARTITIONS);
  }

  /**
   * This function will return true if the input key/value pair is valid.
   */
  protected boolean process(
      INPUT_KEY inputKey,
      INPUT_VALUE inputValue,
      BytesWritable keyBW,
      BytesWritable valueBW,
      Reporter reporter) {
    recordKey = veniceRecordReader.getKeyBytes(inputKey, inputValue);
    recordValue = veniceRecordReader.getValueBytes(inputKey, inputValue);

    if (recordKey == null) {
      throw new VeniceException("Mapper received a empty key record");
    }

    if (recordValue == null) {
      LOGGER.warn("Received null record, skip.");
      MRJobCounterHelper.incrEmptyRecordCount(reporter, 1);
      return false;
    }

    // both key and value are not null: Record uncompressed Key and value lengths
    MRJobCounterHelper.incrTotalKeySize(reporter, recordKey.length);
    MRJobCounterHelper.incrTotalUncompressedValueSize(reporter, recordValue.length);

    // Compress and save the details based on the configured compression strategy: This should not fail
    byte[] finalRecordValue;
    try {
      finalRecordValue = compressor[compressionStrategy.getValue()].compress(recordValue);
    } catch (IOException e) {
      throw new VeniceException(
          "Caught an IO exception while trying to to use compression strategy: "
              + compressor[compressionStrategy.getValue()].getCompressionStrategy().name(),
          e);
    }
    // record the final stored value length
    MRJobCounterHelper.incrTotalValueSize(reporter, finalRecordValue.length);
    keyBW.set(recordKey, 0, recordKey.length);
    valueBW.set(finalRecordValue, 0, finalRecordValue.length);

    if (compressionMetricCollectionEnabled) {
      // Compress based on all compression strategies to collect metrics
      byte[] compressedRecordValue;
      for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
        if (compressionStrategy != NO_OP && compressor[compressionStrategy.getValue()] != null) {
          if (compressionStrategy == this.compressionStrategy) {
            // Extra check to not redo compression
            compressedRecordValue = finalRecordValue;
          } else {
            try {
              compressedRecordValue = compressor[compressionStrategy.getValue()].compress(recordValue);
            } catch (IOException e) {
              LOGGER.warn(
                  "Compression to collect metrics failed for compression strategy: {}",
                  compressionStrategy.name(),
                  e);
              continue;
            }
          }
          switch (compressionStrategy) {
            case GZIP:
              MRJobCounterHelper.incrTotalGzipCompressedValueSize(reporter, compressedRecordValue.length);
              break;

            case ZSTD_WITH_DICT:
              MRJobCounterHelper.incrTotalZstdCompressedValueSize(reporter, compressedRecordValue.length);
              break;

            default:
              // NO_OP won't reach here as its collected already for all cases.
              // ZSTD won't reach here as its deprecated, so not initialized.
              throw new VeniceException(
                  "Support for compression Strategy: " + compressionStrategy.name() + " needs to be added");
          }
        }
      }
    }
    return true;
  }

  /**
   * A method for child classes to setup {@link AbstractVeniceMapper#veniceRecordReader}.
   */
  abstract protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> getRecordReader(VeniceProperties props);

  /**
   * A method for child classes to setup {@link AbstractVeniceMapper#veniceFilterChain}.
   */
  abstract protected FilterChain<INPUT_VALUE> getFilterChain(final VeniceProperties props);

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    this.compressorFactory = new CompressorFactory();
    this.veniceRecordReader = getRecordReader(props);
    if (this.veniceRecordReader == null) {
      throw new VeniceException("Record reader not initialized");
    }

    // init compressor array
    this.compressor = new VeniceCompressor[CompressionStrategy.getCompressionStrategyTypesArrayLength()];
    setupCompression(props);
  }

  /**
   * A => {@link VenicePushJob#COMPRESSION_METRIC_COLLECTION_ENABLED} => if enabled, Compression metrics needs to be collected.
   * check {@link VenicePushJob#evaluateCompressionMetricCollectionEnabled} for more details. <br>
   * B => {@link VenicePushJob#ZSTD_DICTIONARY_CREATION_REQUIRED} => Check {@link VenicePushJob#shouldBuildZstdCompressionDictionary}
   * for more details<br>
   * C => {@link VenicePushJob#ZSTD_DICTIONARY_CREATION_SUCCESS} => Whether Zstd Dictionary creation is a success.<br><br>
   *
   * case 1: A is true <br>
   * case 1a: B is true, C is true => Collect Metrics for all compression strategies. <br>
   * case 1b: B is true, C is False => Collect Metrics for all compression strategies except {@link CompressionStrategy#ZSTD_WITH_DICT} <br>
   * case 1c: B is false => Same as 1b. Currently, this case won't occur <br><br>
   *
   * case 2: A is false => Compression metrics will not be collected <br>
   * case 2a: B is true, C is true => Compression strategy is {@link CompressionStrategy#ZSTD_WITH_DICT} <br>
   * case 2b: B is true, C is false => Should have failed with an exception <br>
   * case 2c: B is false => No compression metrics collection, but compression strategies other than {@link CompressionStrategy#ZSTD_WITH_DICT} could be enabled <br>
   */
  private void setupCompression(VeniceProperties props) {
    compressionStrategy = CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY));

    // A
    compressionMetricCollectionEnabled = props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED);
    // B
    isZstdDictCreationRequired = props.getBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED);
    // C
    isZstdDictCreationSuccess = props.getBoolean(ZSTD_DICTIONARY_CREATION_SUCCESS);

    if (compressionMetricCollectionEnabled) {
      // case 1
      for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
        switch (compressionStrategy) {
          case NO_OP:
          case GZIP:
            this.compressor[compressionStrategy.getValue()] = compressorFactory.getCompressor(compressionStrategy);
            break;

          case ZSTD_WITH_DICT:
            if (isZstdDictCreationRequired && isZstdDictCreationSuccess) {
              // case 1a
              this.compressor[ZSTD_WITH_DICT.getValue()] = getZstdCompressor(props);
            } // else: case 1b or 1c
            break;

          case ZSTD:
            // deprecated
            break;

          default: // defensive check
            throw new VeniceException(
                "Support for compression Strategy: " + compressionStrategy.name() + " needs to be added");
        }
      }
    } else {
      // case 2
      if (compressionStrategy == ZSTD_WITH_DICT) {
        if (isZstdDictCreationRequired && isZstdDictCreationSuccess) {
          // case 2a
          this.compressor[ZSTD_WITH_DICT.getValue()] = getZstdCompressor(props);
        } // else: case 2b
      } else {
        // case 2c
        this.compressor[compressionStrategy.getValue()] = compressorFactory.getCompressor(compressionStrategy);
      }
    }
  }

  /**
   * This function is added to allow it to be mocked for tests.
   * Since mocking this function of an actual object in {@link AbstractTestVeniceMapper#getMapper(int, int, Consumer)}
   * ended up hitting the original function always, added an override for this in {@link TestVeniceAvroMapperClass}.
   */
  protected ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    return DictionaryUtils.readDictionaryFromKafka(topicName, props);
  }

  private VeniceCompressor getZstdCompressor(VeniceProperties props) {
    String topicName = props.getString(TOPIC_PROP);
    ByteBuffer compressionDictionary = readDictionaryFromKafka(topicName, props);
    int compressionLevel = props.getInt(ZSTD_COMPRESSION_LEVEL, Zstd.maxCompressionLevel());

    if (compressionDictionary != null && compressionDictionary.limit() > 0) {
      return compressorFactory.createVersionSpecificCompressorIfNotExist(
          ZSTD_WITH_DICT,
          topicName,
          compressionDictionary.array(),
          compressionLevel);
    }
    return null;
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
    Utils.closeQuietlyWithErrorLogged(veniceFilterChain);
  }
}
