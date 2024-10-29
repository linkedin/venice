package com.linkedin.venice.proxyjob.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_PROXY_COMPUTE_JOB_CLASS;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobCliUtils;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobStatus;
import com.linkedin.venice.proxyjob.datawriter.utils.ProxyJobUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataWriterProxyDriver {
  private static final Logger LOGGER = LogManager.getLogger(DataWriterProxyDriver.class);

  public static void main(String[] args) throws Exception {
    runDataWriterJob(args, true);
  }

  public static void runDataWriterJob(String[] args, boolean killFromShutdownHook) throws Exception {
    CommandLine commandLine = ProxyJobCliUtils.parseArgs(args);
    Properties properties = ProxyJobCliUtils.getPropertiesFromCliArgs(commandLine);
    VeniceProperties props = new VeniceProperties(properties);

    PushJobSetting pushJobSetting = ProxyJobCliUtils.getPushJobSettingFromCliArgs(commandLine);

    String outputPathStr = ProxyJobCliUtils.getOutputDirFromCliArgs(commandLine);
    Path outputPath = new Path(outputPathStr);

    String computeJobClassName = props.getString(DATA_WRITER_PROXY_COMPUTE_JOB_CLASS);

    Class objectClass = ReflectUtils.loadClass(computeJobClassName);
    Validate.isAssignableFrom(DataWriterComputeJob.class, objectClass);
    Class<? extends DataWriterComputeJob> computeJobClass = (Class<? extends DataWriterComputeJob>) objectClass;
    DataWriterComputeJob computeJob = ReflectUtils.callConstructor(computeJobClass, new Class[0], new Object[0]);

    computeJob.configure(props, pushJobSetting);

    // A shutdown hook is needed to kill the compute job to handle the case where the job is killed by the environment.
    Thread computeJobKiller = new Thread(() -> {
      LOGGER.info("Killing compute job from shutdown hook");
      try {
        computeJob.kill();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    if (killFromShutdownHook) {
      Runtime.getRuntime().addShutdownHook(computeJobKiller);
    }

    computeJob.runJob();

    if (killFromShutdownHook) {
      // Job completed, remove the shutdown hook
      Runtime.getRuntime().removeShutdownHook(computeJobKiller);
    }

    ProxyJobStatus status =
        new ProxyJobStatus(computeJob.getTaskTracker(), computeJob.getStatus(), computeJob.getFailureReason());
    ProxyJobUtils.writeJobStatus(outputPath, status);

    Utils.closeQuietlyWithErrorLogged(computeJob);
  }
}
