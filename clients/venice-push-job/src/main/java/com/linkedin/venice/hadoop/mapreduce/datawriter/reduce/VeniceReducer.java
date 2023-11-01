package com.linkedin.venice.hadoop.mapreduce.datawriter.reduce;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.partition.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.mapreduce.engine.DefaultHadoopJobClientProvider;
import com.linkedin.venice.hadoop.mapreduce.engine.HadoopJobClientProvider;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
public class VeniceReducer extends AbstractPartitionWriter
    implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
  public static final String MAP_REDUCE_JOB_ID_PROP = "mapreduce.job.id";
  private static final Logger LOGGER = LogManager.getLogger(VeniceReducer.class);

  private VeniceProperties props;

  private Reporter reporter = null;
  private DataWriterTaskTracker dataWriterTaskTracker = null;
  private HadoopJobClientProvider hadoopJobClientProvider = new DefaultHadoopJobClientProvider();

  private MapReduceEngineTaskConfigProvider engineTaskConfigProvider;

  @Override
  protected EngineTaskConfigProvider getEngineTaskConfigProvider() {
    return engineTaskConfigProvider;
  }

  protected JobConf getJobConf() {
    return engineTaskConfigProvider.getJobConf();
  }

  @Override
  public void configure(JobConf job) {
    this.engineTaskConfigProvider = new MapReduceEngineTaskConfigProvider(job);
    super.configure();
  }

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) {
    if (updatePreviousReporter(reporter)) {
      setCallback(new ReducerProduceCallback(reporter));
      dataWriterTaskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    }
    processValuesForKey(key.copyBytes(), mapIterator(values, BytesWritable::copyBytes), dataWriterTaskTracker);
  }

  private boolean updatePreviousReporter(Reporter reporter) {
    if (this.reporter == null || !this.reporter.equals(reporter)) {
      this.reporter = reporter;
      return true;
    }
    return false;
  }

  // Visible for testing
  @Override
  protected boolean getExceedQuotaFlag() {
    return super.getExceedQuotaFlag();
  }

  // Visible for testing
  @Override
  protected void setVeniceWriter(AbstractVeniceWriter veniceWriter) {
    super.setVeniceWriter(veniceWriter);
  }

  // Visible for testing
  @Override
  protected void setExceedQuota(boolean exceedQuota) {
    super.setExceedQuota(exceedQuota);
  }

  // Visible for testing
  @Override
  protected boolean hasReportedFailure(DataWriterTaskTracker dataWriterTaskTracker, boolean isDuplicateKeyAllowed) {
    return super.hasReportedFailure(dataWriterTaskTracker, isDuplicateKeyAllowed);
  }

  // Visible for testing
  @Override
  protected PubSubProducerCallback getCallback() {
    return super.getCallback();
  }

  @Override
  protected void configureTask(VeniceProperties props) {
    super.configureTask(props);
    this.props = props;
  }

  @Override
  protected long getTotalIncomingDataSizeInBytes() {
    return getTotalIncomingDataSizeInBytes(getJobConf());
  }

  private long getTotalIncomingDataSizeInBytes(JobConf jobConfig) {
    JobClient hadoopJobClient = null;
    String jobIdProp = null;
    JobID jobID = null;
    RunningJob runningJob = null;
    Counters quotaCounters = null;
    try {
      hadoopJobClient = hadoopJobClientProvider.getJobClientFromConfig(jobConfig);
      jobIdProp = jobConfig.get(MAP_REDUCE_JOB_ID_PROP);
      jobID = JobID.forName(jobIdProp);
      runningJob = hadoopJobClient.getJob(jobID);
      quotaCounters = runningJob.getCounters();
      return MRJobCounterHelper.getTotalKeySize(quotaCounters) + MRJobCounterHelper.getTotalValueSize(quotaCounters);

    } catch (Exception e) {
      /**
       * Note that this will catch a NPE during tests unless the storage quota is set to
       * {@link Store.UNLIMITED_STORAGE_QUOTA}, which seems quite messed up. It manifests as {@link runningJob}
       * being null, which then prevents us from calling {@link RunningJob#getCounters()}. Obviously, this is
       * not happening in prod, though it's not completely clear why...
       *
       * TODO: Fix this so that tests are more representative of prod
       */
      throw new VeniceException(
          String.format(
              "Can't read input file size from counters; hadoopJobClient: %s; jobIdProp: %s; jobID: %s; runningJob: %s; quotaCounters: %s",
              hadoopJobClient,
              jobIdProp,
              jobID,
              runningJob,
              quotaCounters),
          e);
    } finally {
      if (hadoopJobClient != null) {
        try {
          hadoopJobClient.close();
        } catch (IOException e) {
          throw new VeniceException("Cannot close hadoopJobClient", e);
        }
      }
    }
  }

  // Visible for testing
  protected void setHadoopJobClientProvider(HadoopJobClientProvider hadoopJobClientProvider) {
    this.hadoopJobClientProvider = hadoopJobClientProvider;
  }

  protected class ReducerProduceCallback implements PubSubProducerCallback {
    private final Reporter reporter;

    public ReducerProduceCallback(Reporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception e) {
      if (e != null) {
        // This is unexpected since it should be handled in AbstractPartitionWriter. Ignore.
        LOGGER.warn("Received exception in pubsub callback. This is unexpected.", e);
      } else {
        int partition = produceResult.getPartition();
        if (partition != getTaskId()) {
          // Reducer input and output are not aligned!
          recordMessageErrored(
              new VeniceException(
                  String.format(
                      "The reducer is not writing to the Kafka partition that maps to its task (taskId = %d, partition = %d). "
                          + "This could mean that MR shuffling is buggy or that the configured %s (%s) is non-deterministic.",
                      getTaskId(),
                      partition,
                      VenicePartitioner.class.getSimpleName(),
                      props.getString(ConfigKeys.PARTITIONER_CLASS))));
        }
      }
      // Report progress so map-reduce framework won't kill current reducer when it finishes
      // sending all the messages to Kafka broker, but not yet flushed and closed.
      reporter.progress();
    }

    protected Progressable getProgressable() {
      return reporter;
    }
  }

  private <T, O> Iterator<O> mapIterator(Iterator<T> iterator, Function<T, O> mapper) {
    return new Iterator<O>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public O next() {
        return mapper.apply(iterator.next());
      }
    };
  }
}
