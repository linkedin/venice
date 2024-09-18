package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ValidateSchemaAndBuildDictMapper;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormatCombiner;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Properties;


/**
 * Class for commonalities between {@link AbstractInputRecordProcessor}, {@link AbstractPartitionWriter},
 * {@link KafkaInputFormatCombiner} and {@link ValidateSchemaAndBuildDictMapper}.
 */
public abstract class AbstractDataWriterTask {
  protected static final int TASK_ID_NOT_SET = -1;

  private int partitionCount;
  private int taskId = TASK_ID_NOT_SET;
  private boolean isChunkingEnabled;
  private boolean isRmdChunkingEnabled;
  private EngineTaskConfigProvider engineTaskConfigProvider;

  /**
   * @return The number of partitions for the pubsub topic where data will be written.
   */
  protected int getPartitionCount() {
    return this.partitionCount;
  }

  /**
   * @return The ID of the current task. This ID number is 0-based.
   */
  protected int getTaskId() {
    return this.taskId;
  }

  protected void setChunkingEnabled(boolean isChunkingEnabled) {
    this.isChunkingEnabled = isChunkingEnabled;
  }

  protected boolean isChunkingEnabled() {
    return isChunkingEnabled;
  }

  protected boolean isRmdChunkingEnabled() {
    return isRmdChunkingEnabled;
  }

  private void verifyEngineTaskConfigProviderConfigured() {
    if (engineTaskConfigProvider == null) {
      throw new IllegalStateException("EngineTaskConfigProvider is not configured. Please call configure() first.");
    }
  }

  protected EngineTaskConfigProvider getEngineTaskConfigProvider() {
    verifyEngineTaskConfigProviderConfigured();
    return engineTaskConfigProvider;
  }

  /**
   * Allow implementations of this class to configure task-specific stuff.
   * @param props the job props that the task was configured with.
   */
  protected abstract void configureTask(VeniceProperties props);

  /**
   * Configures the task with the given {@link EngineTaskConfigProvider}.
   * @param engineTaskConfigProvider
   */
  protected final void configure(EngineTaskConfigProvider engineTaskConfigProvider) {
    this.engineTaskConfigProvider = engineTaskConfigProvider;
    Properties jobProps = engineTaskConfigProvider.getJobProps();
    String sslConfiguratorClassName = jobProps.getProperty(SSL_CONFIGURATOR_CLASS_CONFIG);
    if (sslConfiguratorClassName != null) {
      SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(sslConfiguratorClassName);
      try {
        jobProps = configurator.setupSSLConfig(jobProps, UserCredentialsFactory.getHadoopUserCredentials());
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for job: " + engineTaskConfigProvider.getJobName(), e);
      }
    }
    this.isRmdChunkingEnabled = Boolean.parseBoolean(jobProps.getProperty(VeniceWriter.ENABLE_RMD_CHUNKING));
    setChunkingEnabled(Boolean.parseBoolean(jobProps.getProperty(VeniceWriter.ENABLE_CHUNKING)));
    this.partitionCount = Integer.parseInt(jobProps.getProperty(PARTITION_COUNT));
    this.taskId = engineTaskConfigProvider.getTaskId();
    configureTask(new VeniceProperties(jobProps));
  }
}
