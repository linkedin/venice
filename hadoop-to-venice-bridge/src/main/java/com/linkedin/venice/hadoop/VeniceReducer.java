package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.hadoop.MapReduceConstants.*;

/**
 * {@link VeniceReducer} will be in charge of producing the messages to Kafka broker.
 * Since {@link VeniceMRPartitioner} is using the same logic of {@link com.linkedin.venice.partitioner.DefaultVenicePartitioner},
 * all the messages in the same reducer belongs to the same topic partition.
 *
 * The reason to introduce a reduce phase is that BDB-JE will benefit with sorted input in the following ways:
 * 1. BDB-JE won't generate so many BINDelta since it won't touch a lot of BINs at a time;
 * 2. The overall BDB-JE insert rate will improve a lot since the disk usage will be reduced a lot (BINDelta will be
 * much smaller than before);
 *
 */
public class VeniceReducer implements Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceReducer.class);

  private static final int COUNTER_STATEMENT_COUNT = 100000;

  private long lastTimeChecked = System.currentTimeMillis();

  private AbstractVeniceWriter<byte[], byte[]> veniceWriter = null;
  private int valueSchemaId = -1;

  private boolean isDuplicateKeyAllowed;

  private Exception sendException = null;

  private KafkaMessageCallback callback = null;
  private Reporter previousReporter = null;
  /**
   * This doesn't need to be atomic since {@link #reduce(BytesWritable, Iterator, OutputCollector, Reporter)} will be called sequentially.
   */
  private long messageSent = 0;
  private final AtomicLong messageCompleted = new AtomicLong();
  private final AtomicLong messageErrored = new AtomicLong();


  @Override
  public void reduce(BytesWritable key, Iterator<BytesWritable> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    byte[] keyBytes = key.copyBytes();
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " +
          DatatypeConverter.printHexBinary(keyBytes));
    }
    byte[] valueBytes = values.next().copyBytes();

    sendMessageToKafka(keyBytes, valueBytes, reporter);

    //encounter duplicate key issue
    while (values.hasNext()) {
      if (values.next().copyBytes() == valueBytes) {
        //the data should be fine since both values are identical, only does logging here
        reporter.incrCounter(COUNTER_GROUP_KAFKA, DUPLICATE_KEY_WITH_IDENTICAL_VALUE, 1);
      } else {
        if (!isDuplicateKeyAllowed) {
          throw new VeniceException("There are multiple records for key: " + DatatypeConverter.printHexBinary(keyBytes));
        } else {
          reporter.incrCounter(COUNTER_GROUP_KAFKA, DUPLICATE_KEY_WITH_DISTINCT_VALUE, 1);
        }
      }
    }
  }

  protected void sendMessageToKafka(byte[] keyBytes, byte[] valueBytes, Reporter reporter) {
    maybePropagateCallbackException();
    if (null == veniceWriter) {
      throw new VeniceException("VeniceWriter hasn't been initialized yet!");
    }
    if (null == previousReporter || !previousReporter.equals(reporter)) {
      callback = new KafkaMessageCallback(reporter);
      previousReporter = reporter;
    }
    veniceWriter.put(keyBytes, valueBytes, valueSchemaId, callback);
    ++messageSent;

    if (messageSent % COUNTER_STATEMENT_COUNT == 0) {
      double transferRate = COUNTER_STATEMENT_COUNT * 1000 / (System.currentTimeMillis() - lastTimeChecked);
      LOGGER.info("Record count: " + messageSent + " rec/s:" + transferRate);
      lastTimeChecked = System.currentTimeMillis();
    }

    reporter.incrCounter(COUNTER_GROUP_KAFKA, COUNTER_OUTPUT_RECORDS, 1);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Kafka message progress before flushing and closing producer:");
    logMessageProgress();
    if (null != veniceWriter) {
      veniceWriter.close();
    }
    maybePropagateCallbackException();
    LOGGER.info("Kafka message progress after flushing and closing producer:");
    logMessageProgress();
    if (messageSent != messageCompleted.get()) {
      throw new VeniceException("Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
    }
  }

  @Override
  public void configure(JobConf job) {
    /*
     * Before producing kafka msg, reducer would check storage quota first.
     * Reducer reads input file size from mappers' counter and check it with
     * Venice controller. It stops and throws exception if the quota is exceeded.
     */
    VeniceProperties props = HadoopUtils.getVeniceProps(job);

    prepushStorageQuotaCheck(job, props.getLong(STORAGE_QUOTA_PROP), props.getDouble(STORAGE_ENGINE_OVERHEAD_RATIO));


    this.valueSchemaId = props.getInt(VALUE_SCHEMA_ID_PROP);
    this.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY);
    if (null == this.veniceWriter) {
      this.veniceWriter = new VeniceWriter<>(
          props,
          props.getString(TOPIC_PROP),
          new DefaultSerializer(),
          new DefaultSerializer()
      );
    }
  }

  private void prepushStorageQuotaCheck(JobConf job, long maxStorageQuota, double storageEngineOverheadRatio) {
    //skip the check if the quota is unlimited
    if (maxStorageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      return;
    }

    JobClient hadoopJobClient = null;

    try {
      hadoopJobClient = new JobClient(job);
      Counters quotaCounters =
          hadoopJobClient.getJob(JobID.forName(job.get("mapred.job.id"))).getCounters();
      long incomingDataSize =
          estimatedVeniceDiskUsage(quotaCounters.findCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE).getValue() +
              quotaCounters.findCounter(COUNTER_GROUP_QUOTA,COUNTER_TOTAL_VALUE_SIZE).getValue(), storageEngineOverheadRatio);

      if (incomingDataSize > maxStorageQuota) {
        throw new QuotaExceededException("Venice reducer data push",
                  Long.toString(incomingDataSize), Long.toString(maxStorageQuota));
      }
    } catch (IOException e) {
      throw new VeniceException("Can't read input file size from counters", e);
    } finally {
      if (hadoopJobClient != null) {
        try {
          hadoopJobClient.close();
        } catch (IOException e) {
          throw new VeniceException("Can't close hadoopJobClient", e);
        }
      }
    }
  }

  private long estimatedVeniceDiskUsage(long inputFileSize, double storageEngineOverheadRatio) {
    return (long) (inputFileSize / storageEngineOverheadRatio);
  }

  private void maybePropagateCallbackException() {
    if (null != sendException) {
      throw new VeniceException("KafkaPushJob failed with exception", sendException);
    }
  }

  private void logMessageProgress() {
    LOGGER.info("Message sent: " + messageSent);
    LOGGER.info("Message completed: " + messageCompleted.get());
    LOGGER.info("Message errored: " + messageErrored.get());
  }

  // For test purpose
  protected void setVeniceWriter(AbstractVeniceWriter veniceWriter) {
    this.veniceWriter = veniceWriter;
  }


  protected class KafkaMessageCallback implements Callback {
    private final Progressable progress;

    public KafkaMessageCallback(Progressable progress) {
      this.progress = progress;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (null != e) {
        messageErrored.incrementAndGet();
        sendException = e;
      } else {
        messageCompleted.incrementAndGet();
      }
      // Report progress so map-reduce framework won't kill current reducer when it finishes
      // sending all the messages to Kafka broker, but not yet flushed and closed.
      progress.progress();
    }

    protected Progressable getProgressable() {
      return progress;
    }
  }
}
