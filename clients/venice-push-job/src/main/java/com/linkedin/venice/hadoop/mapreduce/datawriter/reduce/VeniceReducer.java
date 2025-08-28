package com.linkedin.venice.hadoop.mapreduce.datawriter.reduce;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.partition.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.mapreduce.engine.DefaultHadoopJobClientProvider;
import com.linkedin.venice.hadoop.mapreduce.engine.HadoopJobClientProvider;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.IteratorUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;


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

  private Reporter reporter = null;
  private HadoopJobClientProvider hadoopJobClientProvider = new DefaultHadoopJobClientProvider();
  private JobConf jobConf;

  protected JobConf getJobConf() {
    return jobConf;
  }

  @Override
  public void configure(JobConf job) {
    this.jobConf = job;
    super.configure(new MapReduceEngineTaskConfigProvider(job));
  }

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) {
    DataWriterTaskTracker dataWriterTaskTracker;
    if (updatePreviousReporter(reporter)) {
      dataWriterTaskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    } else {
      dataWriterTaskTracker = getDataWriterTaskTracker();
    }

    processValuesForKey(
        key.copyBytes(),
        IteratorUtils.mapIterator(values, x -> new VeniceRecordWithMetadata(x.copyBytes(), null)),
        dataWriterTaskTracker);
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
  protected DataWriterTaskTracker getDataWriterTaskTracker() {
    return super.getDataWriterTaskTracker();
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

  // Visible for testing
  @Override
  protected AbstractVeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
    return super.createBasicVeniceWriter();
  }

  // Visible for testing
  @Override
  protected void setVeniceWriterFactory(VeniceWriterFactory factory) {
    super.setVeniceWriterFactory(factory);
  }
}
