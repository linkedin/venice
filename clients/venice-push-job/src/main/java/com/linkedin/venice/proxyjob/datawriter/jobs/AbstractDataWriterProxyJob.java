package com.linkedin.venice.proxyjob.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PERMISSION_700;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.proxyjob.datawriter.task.DelegatingReadOnlyDataWriterTaskTracker;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobCliUtils;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobStatus;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This will be the proxy job for the data writer job. It will launch the data writer job in an isolated environment,
 * like a new process, or a remote execution environment. It will act as a proxy between the data writer job and the VPJ
 * driver.
 */
public abstract class AbstractDataWriterProxyJob extends DataWriterComputeJob {
  private static final Logger LOGGER = LogManager.getLogger(AbstractDataWriterProxyJob.class);
  private VeniceProperties props;
  private PushJobSetting pushJobSetting;
  private DelegatingReadOnlyDataWriterTaskTracker taskTracker;

  @Override
  public DataWriterTaskTracker getTaskTracker() {
    return taskTracker;
  }

  protected abstract void runProxyJob(List<String> args);

  @Override
  protected final void runComputeJob() {
    String proxyJobStateDir = Utils.escapeFilePathComponent(Utils.getUniqueString("data_writer_proxy_job"));
    Path proxyJobStateDirPath = new Path(pushJobSetting.jobTmpDir, proxyJobStateDir);
    try {
      HadoopUtils.createDirectoryWithPermission(proxyJobStateDirPath, PERMISSION_700);
      List<String> args =
          ProxyJobCliUtils.toCliArgs(props.toProperties(), pushJobSetting, proxyJobStateDirPath.toUri().toString());
      runProxyJob(args);

      ProxyJobStatus jobStatus = ProxyJobUtils.getJobStatus(proxyJobStateDirPath);
      if (jobStatus == null) {
        throw new RuntimeException("Failed to get job status from proxy job state directory: " + proxyJobStateDirPath);
      }

      taskTracker.setDelegate(jobStatus.getTaskTracker());
      logJobMetrics(jobStatus.getTaskTracker());
      if (jobStatus.getFailureReason() != null) {
        throw new RuntimeException(jobStatus.getFailureReason());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create proxy job state directory: " + proxyJobStateDirPath, e);
    }
  }

  private void logJobMetrics(DataWriterTaskTracker taskTracker) {
    LOGGER.info("Job metrics for data writer job:");
    LOGGER.info("  Total Output Records: {}", taskTracker.getOutputRecordsCount());
    LOGGER.info("  Empty Records: {}", taskTracker.getEmptyRecordCount());
    LOGGER.info("  Total Key Size: {}", taskTracker.getTotalKeySize());
    LOGGER.info("  Total Uncompressed Value Size: {}", taskTracker.getTotalUncompressedValueSize());
    LOGGER.info("  Total Compressed Value Size: {}", taskTracker.getTotalValueSize());
    LOGGER.info("  Total Gzip Compressed Value Size: {}", taskTracker.getTotalGzipCompressedValueSize());
    LOGGER.info("  Total Zstd Compressed Value Size: {}", taskTracker.getTotalZstdCompressedValueSize());
    LOGGER.info("  Spray All Partitions Triggered: {}", taskTracker.getSprayAllPartitionsCount());
    LOGGER.info("  Partition Writers Closed: {}", taskTracker.getPartitionWriterCloseCount());
    LOGGER.info("  Repush TTL Filtered Records: {}", taskTracker.getRepushTtlFilterCount());
    LOGGER.info("  ACL Authorization Failures: {}", taskTracker.getWriteAclAuthorizationFailureCount());
    LOGGER.info("  Record Too Large Failures: {}", taskTracker.getRecordTooLargeFailureCount());
    LOGGER.info("  Duplicate Key With Identical Value: {}", taskTracker.getDuplicateKeyWithIdenticalValueCount());
    LOGGER.info("  Duplicate Key With Distinct Value: {}", taskTracker.getDuplicateKeyWithDistinctValueCount());
  }

  @Override
  public VeniceProperties getJobProperties() {
    return props;
  }

  @Override
  protected PushJobSetting getPushJobSetting() {
    return pushJobSetting;
  }

  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    this.props = props;
    this.pushJobSetting = pushJobSetting;
    this.taskTracker = new DelegatingReadOnlyDataWriterTaskTracker();
  }
}
