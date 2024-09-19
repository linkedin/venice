package com.linkedin.venice.hadoop.mapreduce;

import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_SUCCESS;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.VeniceReducer;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.DataProvider;


public class AbstractTestVeniceMR {
  private static final double DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO = 0.85d;

  protected static final String SCHEMA_STR = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"TestRecord\",\n"
      + "\t\"fields\": [\n" + "\t\t{\"name\": \"key\", \"type\": \"string\"},\n"
      + "\t\t{\"name\": \"value\", \"type\": \"string\"}\n" + "\t]\n" + "}";
  protected static final String KEY_FIELD = "key";
  protected static final String VALUE_FIELD = "value";
  protected static final int VALUE_SCHEMA_ID = 1;

  protected static final String TOPIC_NAME = "test_store_v1";

  protected static final String MAPPER_PARAMS_DATA_PROVIDER = "mapperParams";

  @DataProvider(name = MAPPER_PARAMS_DATA_PROVIDER)
  public static Object[][] mapperParams() {
    List<Integer> numReducersValues = Arrays.asList(1, 10, 1000);
    List<Integer> taskIdValues = Arrays.asList(0, 1, 10, 1000);
    Object[][] params = new Object[numReducersValues.size() * taskIdValues.size()][2];
    int paramsIndex = 0;
    for (int numReducers: numReducersValues) {
      for (int taskId: taskIdValues) {
        params[paramsIndex][0] = numReducers;
        params[paramsIndex++][1] = taskId;
      }
    }
    return params;
  }

  protected JobConf setupJobConf(int partitionCount) {
    return new JobConf(getDefaultJobConfiguration(partitionCount));
  }

  protected Configuration getDefaultJobConfiguration(int partitionCount) {
    Configuration config = new Configuration();
    config.set(TOPIC_PROP, TOPIC_NAME);
    config.set(KEY_FIELD_PROP, KEY_FIELD);
    config.set(VALUE_FIELD_PROP, VALUE_FIELD);
    config.set(SCHEMA_STRING_PROP, SCHEMA_STR);
    config.setInt(VALUE_SCHEMA_ID_PROP, VALUE_SCHEMA_ID);
    config.setLong(STORAGE_QUOTA_PROP, Store.UNLIMITED_STORAGE_QUOTA);
    config.setDouble(STORAGE_ENGINE_OVERHEAD_RATIO, DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO);
    config.setBoolean(ALLOW_DUPLICATE_KEY, false);
    config.set(COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());
    config.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, false);
    config.setBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, false);
    config.setBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED, false);
    config.setBoolean(ZSTD_DICTIONARY_CREATION_SUCCESS, false);
    config.set(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName());
    config.set(SSL_KEY_STORE_PROPERTY_NAME, "ssl.identity");
    config.set(SSL_TRUST_STORE_PROPERTY_NAME, "ssl.truststore");
    config.set(VeniceReducer.MAP_REDUCE_JOB_ID_PROP, "job_200707121733_0003");
    config.setBoolean(VeniceWriter.ENABLE_CHUNKING, false);
    config.setInt(PARTITION_COUNT, partitionCount);
    return new JobConf(config);
  }

  public static String getHexString(byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes);
  }
}
