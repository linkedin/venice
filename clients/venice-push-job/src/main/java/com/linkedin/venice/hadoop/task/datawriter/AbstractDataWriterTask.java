package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.AbstractMapReduceTask;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Properties;


/**
 * Class for commonalities between {@link AbstractMapReduceTask}, {@link AbstractInputRecordProcessor} and {@link AbstractPartitionWriter}.
 */
public abstract class AbstractDataWriterTask {
  protected static final int TASK_ID_NOT_SET = -1;

  private int partitionCount;
  private int taskId = TASK_ID_NOT_SET;
  private boolean isChunkingEnabled;
  private boolean isRmdChunkingEnabled;

  protected int getPartitionCount() {
    return this.partitionCount;
  }

  protected int getTaskId() {
    return this.taskId;
  }

  protected void setChunkingEnabled(boolean isChunkingEnabled) {
    this.isChunkingEnabled = isChunkingEnabled;
  }

  protected boolean isChunkingEnabled() {
    return isChunkingEnabled;
  }

  protected void setRmdChunkingEnabled(boolean isRmdChunkingEnabled) {
    this.isRmdChunkingEnabled = isRmdChunkingEnabled;
  }

  protected boolean isRmdChunkingEnabled() {
    return isRmdChunkingEnabled;
  }

  protected abstract EngineTaskConfigProvider getEngineTaskConfigProvider();

  protected abstract void configureTask(VeniceProperties props);

  // TODO: Find a better name
  protected final void configure() {
    EngineTaskConfigProvider engineTaskConfigProvider = getEngineTaskConfigProvider();
    Properties jobProps = engineTaskConfigProvider.getJobProps();
    String sslConfiguratorClassName = jobProps.getProperty(SSL_CONFIGURATOR_CLASS_CONFIG);
    if (sslConfiguratorClassName != null) {
      SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(sslConfiguratorClassName);
      try {
        jobProps = configurator.setupSSLConfig(jobProps, UserCredentialsFactory.getHadoopUserCredentials());
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for job:" + engineTaskConfigProvider.getJobName(), e);
      }
    }
    setRmdChunkingEnabled(Boolean.parseBoolean(jobProps.getProperty(VeniceWriter.ENABLE_RMD_CHUNKING)));
    setChunkingEnabled(Boolean.parseBoolean(jobProps.getProperty(VeniceWriter.ENABLE_CHUNKING)));
    this.partitionCount = Integer.parseInt(jobProps.getProperty(PARTITION_COUNT));
    this.taskId = engineTaskConfigProvider.getTaskId();
    configureTask(new VeniceProperties(jobProps));
  }
}
