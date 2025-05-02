package com.linkedin.venice.jobs;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputRecordReader;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstraction for executing and monitoring a data writer compute job running on any batch compute engine
 */
public abstract class DataWriterComputeJob implements ComputeJob {
  private static final Logger LOGGER = LogManager.getLogger(DataWriterComputeJob.class);

  /**
   * Pass-through the properties whose names start with:
   * <ul>
   *   <li> {@link VeniceWriter#VENICE_WRITER_CONFIG_PREFIX} </li>
   *   <li> {@link ConfigKeys#KAFKA_CONFIG_PREFIX} </li>
   *   <li> {@link KafkaInputRecordReader#KIF_RECORD_READER_KAFKA_CONFIG_PREFIX} </li>
   * </ul>
   **/
  public static final List<String> PASS_THROUGH_CONFIG_PREFIXES = Collections.unmodifiableList(
      Arrays.asList(
          VeniceWriter.VENICE_WRITER_CONFIG_PREFIX,
          ConfigKeys.KAFKA_CONFIG_PREFIX,
          ConfigKeys.PUBSUB_CLIENT_CONFIG_PREFIX,
          KafkaInputRecordReader.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX));

  private Status jobStatus = Status.NOT_STARTED;
  private Throwable failureReason = null;

  public abstract DataWriterTaskTracker getTaskTracker();

  protected void validateJob() {
    DataWriterTaskTracker dataWriterTaskTracker = getTaskTracker();
    if (dataWriterTaskTracker == null) {
      throw new VeniceException("DataWriterTaskTracker is not set. Unable to validate the job status.");
    }

    PushJobSetting pushJobSetting = getPushJobSetting();

    if (pushJobSetting.inputHasRecords) {
      final long taskTrackerClosedCount = dataWriterTaskTracker.getPartitionWriterCloseCount();
      if (pushJobSetting.isSourceKafka && pushJobSetting.repushTTLEnabled) {
        LOGGER.info("Repush with ttl filtered out {} records", dataWriterTaskTracker.getRepushTtlFilterCount());
      }
      if (taskTrackerClosedCount < pushJobSetting.partitionCount) {
        /**
         * No reducer tasks gets created if there is no data record present in source kafka topic in Kafka Input Format mode.
         * This is possible if the current version is created because of an empty push. Let's check to make sure this is
         * indeed the case and don't fail the job and instead let the job continue. This will basically be equivalent of
         * another empty push which will create a new version.
         */
        if (pushJobSetting.isSourceKafka) {
          long totalPutOrDeleteRecordsCount = dataWriterTaskTracker.getTotalPutOrDeleteRecordsCount();
          LOGGER.info(
              "Source kafka input topic : {} has {} records",
              pushJobSetting.kafkaInputTopic,
              totalPutOrDeleteRecordsCount);
          if (totalPutOrDeleteRecordsCount == 0) {
            return;
          }
        }
        if (dataWriterTaskTracker.getSprayAllPartitionsCount() == 0) {
          /**
           * Right now, only the mapper with task id: 0 will spray all the partitions to make sure each reducer will
           * be instantiated.
           * In some situation, it is possible the mapper with task id: 0 won't receive any records, so this
           * {@link AbstractVeniceMapper#maybeSprayAllPartitions} won't be invoked, so it is not guaranteed that
           * each reducer will be invoked, and the closing event of the reducers won't be tracked by {@link Reporter},
           * which will only be passed via {@link org.apache.hadoop.mapreduce.Reducer#reduce}.
           *
           * It will require a lot of efforts to make sure {@link AbstractVeniceMapper#maybeSprayAllPartitions} will be
           * invoked in all the scenarios, so right now, we choose this approach:
           * When {@link AbstractVeniceMapper#maybeSprayAllPartitions} is not invoked, VPJ won't fail if the reducer
           * close count is smaller than the partition count since we couldn't differentiate whether it is a real issue
           * or not.
           *
           * If there is a need to make it work for all the cases, and here are the potential proposals:
           * 1. Invoke {@link AbstractVeniceMapper#maybeSprayAllPartitions} in every mapper.
           * 2. Fake some input record to make sure the first mapper would always receive some message.
           * 3. Examine the VT to find out how many partitions contain messages from batch push.
           */
          LOGGER.warn(
              "'AbstractVeniceMapper#maybeSprayAllPartitions' is not invoked, so we couldn't"
                  + " decide whether the push job finished successfully or not purely based on the reducer job"
                  + " closed count ({}) < the partition count ({})",
              taskTrackerClosedCount,
              pushJobSetting.partitionCount);
          return;
        }

        throw new VeniceException(
            String.format(
                "Task tracker is not reliable since the partition writer closed count (%d) < the partition count (%d), "
                    + "while the input file data size is %d byte(s)",
                taskTrackerClosedCount,
                pushJobSetting.partitionCount,
                pushJobSetting.inputFileDataSizeInBytes));
      }
    } else {
      verifyTaskWithZeroValues(dataWriterTaskTracker);
    }
  }

  protected abstract void runComputeJob();

  @Override
  public void configure(VeniceProperties properties) {
    LOGGER.warn("Data writer compute job needs additional configs to be configured.");
  }

  protected abstract PushJobSetting getPushJobSetting();

  public abstract void configure(VeniceProperties props, PushJobSetting pushJobSetting);

  @Override
  public void runJob() {
    try {
      jobStatus = Status.RUNNING;
      runComputeJob();
    } catch (Throwable e) {
      failureReason = e;
      // If the job has already been killed
      if (jobStatus == Status.KILLED) {
        return;
      }
      jobStatus = Status.FAILED;
      return;
    }

    try {
      validateJob();
    } catch (Throwable e) {
      failureReason = e;
      jobStatus = Status.FAILED_VERIFICATION;
      return;
    }

    jobStatus = Status.SUCCEEDED;
  }

  @Override
  public void kill() {
    this.jobStatus = Status.KILLED;
  }

  @Override
  public Status getStatus() {
    return jobStatus;
  }

  @Override
  public Throwable getFailureReason() {
    return failureReason;
  }

  private void verifyTaskWithZeroValues(DataWriterTaskTracker dataWriterTaskTracker) {
    final long outputRecordsCount = dataWriterTaskTracker.getOutputRecordsCount();
    if (outputRecordsCount != 0) {
      throw new VeniceException("Expect 0 output record. Got count: " + outputRecordsCount);
    }
    final long writeAclAuthorizationFailureCount = dataWriterTaskTracker.getWriteAclAuthorizationFailureCount();
    if (writeAclAuthorizationFailureCount != 0) {
      throw new VeniceException("Expect 0 ACL authorization failure. Got count: " + writeAclAuthorizationFailureCount);
    }
    final long duplicateKeyWithDistinctCount = dataWriterTaskTracker.getDuplicateKeyWithDistinctValueCount();
    if (duplicateKeyWithDistinctCount != 0) {
      throw new VeniceException(
          "Expect 0 duplicated key with distinct value. Got count: " + duplicateKeyWithDistinctCount);
    }
    final long totalKeySize = dataWriterTaskTracker.getTotalKeySize();
    if (totalKeySize != 0) {
      throw new VeniceException("Expect 0 byte for total key size. Got count: " + totalKeySize);
    }
    final long totalValueSize = dataWriterTaskTracker.getTotalValueSize();
    if (totalValueSize != 0) {
      throw new VeniceException("Expect 0 byte for total value size. Got count: " + totalValueSize);
    }
  }
}
