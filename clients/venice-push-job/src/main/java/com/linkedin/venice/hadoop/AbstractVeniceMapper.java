package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_STRATEGY;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.ZSTD_COMPRESSION_LEVEL;

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
import java.util.Optional;
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

  private CompressorFactory compressorFactory;
  private VeniceCompressor compressor;
  byte[] recordKey = null, recordValue = null;
  BytesWritable keyBW = new BytesWritable(), valueBW = new BytesWritable();

  protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> veniceRecordReader;
  protected Optional<AbstractVeniceFilter<INPUT_VALUE>> veniceFilter;

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
    MRJobCounterHelper.incrTotalUncompressedValueSize(reporter, recordValue.length);

    try {
      recordValue = compressor.compress(recordValue);
    } catch (IOException e) {
      throw new VeniceException(
          "Caught an IO exception while trying to to use compression strategy: "
              + compressor.getCompressionStrategy().name(),
          e);
    }
    MRJobCounterHelper.incrTotalKeySize(reporter, recordKey.length);
    MRJobCounterHelper.incrTotalValueSize(reporter, recordValue.length);
    keyBW.set(recordKey, 0, recordKey.length);
    valueBW.set(recordValue, 0, recordValue.length);
    return true;
  }

  /**
   * A method for child classes to setup {@link AbstractVeniceMapper#veniceRecordReader}.
   */
  abstract protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> getRecordReader(VeniceProperties props);

  /**
   * A method for child classes to setup {@link AbstractVeniceMapper#veniceFilter}.
   */
  abstract protected Optional<AbstractVeniceFilter<INPUT_VALUE>> getFilter(final VeniceProperties props);

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    this.compressorFactory = new CompressorFactory();
    this.veniceRecordReader = getRecordReader(props);
    if (this.veniceRecordReader == null) {
      throw new VeniceException("Record reader not initialized");
    }

    if (CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)) == CompressionStrategy.ZSTD_WITH_DICT) {
      String topicName = props.getString(TOPIC_PROP);
      ByteBuffer compressionDictionary = DictionaryUtils.readDictionaryFromKafka(topicName, props);
      int compressionLevel = props.getInt(ZSTD_COMPRESSION_LEVEL, Zstd.maxCompressionLevel());

      if (compressionDictionary != null && compressionDictionary.limit() > 0) {
        this.compressor =
            compressorFactory.createCompressorWithDictionary(compressionDictionary.array(), compressionLevel);
      }
    } else {
      this.compressor =
          compressorFactory.getCompressor(CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)));
    }
  }

  @Override
  public void close() {
    if (compressor != null) {
      if (compressor.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT) {
        compressorFactory.removeVersionSpecificCompressor(veniceRecordReader.topicName);
      } else {
        Utils.closeQuietlyWithErrorLogged(compressor);
      }
    }
  }
}
