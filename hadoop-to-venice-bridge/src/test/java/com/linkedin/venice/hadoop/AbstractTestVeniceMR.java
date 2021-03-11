package com.linkedin.venice.hadoop;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.meta.Store;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import javax.xml.bind.DatatypeConverter;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

public class AbstractTestVeniceMR {
  protected static final String SCHEMA_STR = "{\n" +
      "\t\"type\": \"record\",\n" +
      "\t\"name\": \"TestRecord\",\n" +
      "\t\"fields\": [\n" +
      "\t\t{\"name\": \"key\", \"type\": \"string\"},\n" +
      "\t\t{\"name\": \"value\", \"type\": \"string\"}\n" +
      "\t]\n" +
      "}";
  protected static final String KEY_FIELD = "key";
  protected static final String VALUE_FIELD = "value";
  protected static final int VALUE_SCHEMA_ID = 1;

  protected static final String TOPIC_NAME = "test_store_v1";

  protected JobConf setupJobConf() {
    return new JobConf(getDefaultJobConfiguration());
  }

  protected Configuration getDefaultJobConfiguration() {
    Configuration defaultJobConfiguration = new Configuration();
    defaultJobConfiguration.set(TOPIC_PROP, TOPIC_NAME);
    defaultJobConfiguration.set(KEY_FIELD_PROP, KEY_FIELD);
    defaultJobConfiguration.set(VALUE_FIELD_PROP, VALUE_FIELD);
    defaultJobConfiguration.set(SCHEMA_STRING_PROP, SCHEMA_STR);
    defaultJobConfiguration.setInt(VALUE_SCHEMA_ID_PROP, VALUE_SCHEMA_ID);
    defaultJobConfiguration.setLong(STORAGE_QUOTA_PROP, Store.UNLIMITED_STORAGE_QUOTA);
    defaultJobConfiguration.setDouble(STORAGE_ENGINE_OVERHEAD_RATIO, VeniceControllerWrapper.DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO);
    defaultJobConfiguration.setBoolean(VENICE_MAP_ONLY, false);
    defaultJobConfiguration.setBoolean(ALLOW_DUPLICATE_KEY, false);
    defaultJobConfiguration.set(COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());
    defaultJobConfiguration.set(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName());
    defaultJobConfiguration.set(SSL_KEY_STORE_PROPERTY_NAME, "li.datavault.identity");
    defaultJobConfiguration.set(SSL_TRUST_STORE_PROPERTY_NAME, "li.datavault.truststore");
    defaultJobConfiguration.set(VeniceReducer.MAP_REDUCE_JOB_ID_PROP, "job_200707121733_0003");
    defaultJobConfiguration.set(REDUCER_MINIMUM_LOGGING_INTERVAL_MS, "180000");
    return defaultJobConfiguration;
  }

  public static String getHexString(byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes);
  }
}
