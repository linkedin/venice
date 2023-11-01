package com.linkedin.venice.hadoop.mapreduce.datawriter.task;

import com.linkedin.venice.hadoop.ValidateSchemaAndBuildDictMapper;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormatCombiner;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractDataWriterTask;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;


/**
 * Class for commonalities between {@link ValidateSchemaAndBuildDictMapper} and {@link KafkaInputFormatCombiner}.
 */
public abstract class AbstractMapReduceTask extends AbstractDataWriterTask implements JobConfigurable {
  private MapReduceEngineTaskConfigProvider engineTaskConfigProvider = null;

  private void verifyEngineTaskConfigProviderConfigured() {
    if (engineTaskConfigProvider == null) {
      throw new IllegalStateException("EngineTaskConfigProvider is not initialized yet");
    }
  }

  @Override
  protected EngineTaskConfigProvider getEngineTaskConfigProvider() {
    verifyEngineTaskConfigProviderConfigured();
    return engineTaskConfigProvider;
  }

  protected JobConf getJobConf() {
    verifyEngineTaskConfigProviderConfigured();
    return engineTaskConfigProvider.getJobConf();
  }

  @Override
  public void configure(JobConf job) {
    this.engineTaskConfigProvider = new MapReduceEngineTaskConfigProvider(job);
    super.configure();
  }
}
