package com.linkedin.venice.hadoop.mapreduce.common;

import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import com.linkedin.venice.hadoop.JobClientWrapper;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.mapreduce.engine.DefaultJobClientWrapper;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class JobUtils {
  private static final Logger LOGGER = LogManager.getLogger(JobUtils.class);

  private JobUtils() {
  }

  /**
   * Common configuration for all the Mapreduce Jobs run as part of VPJ
   *
   * @param conf
   * @param jobName
   */
  public static void setupCommonJobConf(
      VeniceProperties props,
      JobConf conf,
      String jobName,
      PushJobSetting pushJobSetting) {
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }
    conf.setJobName(jobName);
    conf.setJarByClass(pushJobSetting.vpjEntryClass);
    if (pushJobSetting.enableSSL) {
      conf.set(
          SSL_CONFIGURATOR_CLASS_CONFIG,
          props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
      conf.set(SSL_KEY_STORE_PROPERTY_NAME, props.getString(SSL_KEY_STORE_PROPERTY_NAME));
      conf.set(SSL_TRUST_STORE_PROPERTY_NAME, props.getString(SSL_TRUST_STORE_PROPERTY_NAME));
      conf.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));
    }

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    LOGGER.info("{}: {}", MAPREDUCE_JOB_CLASSLOADER, conf.get(MAPREDUCE_JOB_CLASSLOADER));

    /** Not writing anything to the output for key and value and so the format is Null:
     * Can be overwritten later for specific settings */
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
    conf.setOutputFormat(NullOutputFormat.class);

    /** compression related common configs */
    conf.setBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, pushJobSetting.compressionMetricCollectionEnabled);
    conf.setBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED, pushJobSetting.isZstdDictCreationRequired);
  }

  public static RunningJob runJobWithConfig(JobConf jobConf, JobClientWrapper jobClientWrapper) throws IOException {
    if (jobClientWrapper == null) {
      jobClientWrapper = new DefaultJobClientWrapper();
    }
    return jobClientWrapper.runJobWithConfig(jobConf);
  }
}
