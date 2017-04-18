package com.linkedin.venice.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import javax.xml.bind.DatatypeConverter;

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
  protected static final String TOPIC_NAME = "test_topic";

  protected JobConf setupJobConf() {
    Configuration config = new Configuration();
    config.set(KafkaPushJob.TOPIC_PROP, TOPIC_NAME);
    config.set(KafkaPushJob.AVRO_KEY_FIELD_PROP, KEY_FIELD);
    config.set(KafkaPushJob.AVRO_VALUE_FIELD_PROP, VALUE_FIELD);
    config.set(KafkaPushJob.SCHEMA_STRING_PROP, SCHEMA_STR);
    config.setInt(KafkaPushJob.VALUE_SCHEMA_ID_PROP, VALUE_SCHEMA_ID);
    return new JobConf(config);
  }

  public static String getHexString(byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes);
  }
}
