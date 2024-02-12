package com.linkedin.venice.hadoop.mapreduce.datawriter.map;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractInputRecordProcessor;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import java.io.IOException;
import java.util.function.BiConsumer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * An abstraction of the mapper that would return serialized, and potentially
 * compressed, Avro key/value pairs.
 *
 * @param <INPUT_KEY> type of the input key read from InputFormat
 * @param <INPUT_VALUE> type of the input value read from InputFormat
 */

public abstract class AbstractVeniceMapper<INPUT_KEY, INPUT_VALUE>
    extends AbstractInputRecordProcessor<INPUT_KEY, INPUT_VALUE>
    implements Mapper<INPUT_KEY, INPUT_VALUE, BytesWritable, BytesWritable> {
  private Reporter reporter = null;
  private DataWriterTaskTracker dataWriterTaskTracker = null;

  @Override
  public void map(
      INPUT_KEY inputKey,
      INPUT_VALUE inputValue,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) throws IOException {
    if (updatePreviousReporter(reporter)) {
      dataWriterTaskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    }
    super.processRecord(inputKey, inputValue, getRecordEmitter(output), dataWriterTaskTracker);
  }

  private boolean updatePreviousReporter(Reporter reporter) {
    if (this.reporter == null || !this.reporter.equals(reporter)) {
      this.reporter = reporter;
      return true;
    }
    return false;
  }

  @Override
  public void configure(JobConf job) {
    super.configure(new MapReduceEngineTaskConfigProvider(job));
  }

  private BytesWritable wrapBytes(byte[] data) {
    BytesWritable bw = new BytesWritable();
    if (data == null || data.length == 0) {
      bw.setSize(0);
    } else {
      bw.set(data, 0, data.length);
    }
    return bw;
  }

  private BiConsumer<byte[], byte[]> getRecordEmitter(OutputCollector<BytesWritable, BytesWritable> outputCollector) {
    return (key, value) -> {
      BytesWritable keyBW = wrapBytes(key);
      BytesWritable valueBW = wrapBytes(value);
      try {
        outputCollector.collect(keyBW, valueBW);
      } catch (IOException e) {
        throw new VeniceException(e);
      }
    };
  }
}
