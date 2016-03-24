package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.Utils;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * An {@link org.apache.hadoop.mapred.OutputFormat} implementation which instantiates
 * and configures an {@link com.linkedin.venice.hadoop.AvroKafkaRecordWriter} in order
 * to write a job's output into Kafka.
 */
public class AvroKafkaOutputFormat implements OutputFormat<AvroWrapper<IndexedRecord>, NullWritable> {
  private Logger logger = Logger.getLogger(KafkaPushJob.class);

  public AvroKafkaOutputFormat() {
    super();
  }


  @Override
  public RecordWriter<AvroWrapper<IndexedRecord>, NullWritable> getRecordWriter(FileSystem fileSystem,
                                                                                JobConf conf,
                                                                                String arg2,
                                                                                Progressable progress) throws IOException {
    Properties props = new Properties();
    props.put("metadata.broker.list", conf.get(KafkaPushJob.KAFKA_URL_PROP));
    props.put("kafka.bootstrap.servers", conf.get(KafkaPushJob.KAFKA_URL_PROP));
    props = setPropertiesFromConf(conf, props);
    return new AvroKafkaRecordWriter(props);
  }

  /**
   * Adds all the custom values for the
   */
  public Properties setPropertiesFromConf(JobConf conf, Properties props) {

    // TODO: Bridge should send the storeName, size and schema information to the
    // Controller and retrieve kafka topic, kafka broker url, kafka props for producer,
    // key, value schema id
    // TODO: jobId can be computed by the H2V bridge, probably a randomId will do.
    String kafkaTopic = conf.get(KafkaPushJob.TOPIC_PROP);
    if (kafkaTopic != null) {
      props.put(KafkaPushJob.TOPIC_PROP, conf.get(KafkaPushJob.TOPIC_PROP));
      logger.info(KafkaPushJob.TOPIC_PROP + ": " + kafkaTopic);
    }

    props.put(KafkaPushJob.BATCH_NUM_BYTES_PROP, conf.get(KafkaPushJob.BATCH_NUM_BYTES_PROP)); // size of data to be sent in one batch
    props.put(KafkaPushJob.AVRO_KEY_FIELD_PROP, conf.get(KafkaPushJob.AVRO_KEY_FIELD_PROP));
    props.put(KafkaPushJob.AVRO_VALUE_FIELD_PROP, conf.get(KafkaPushJob.AVRO_VALUE_FIELD_PROP));

    String storeName = conf.get(KafkaPushJob.VENICE_STORE_NAME_PROP);
    if(!Utils.isNullOrEmpty(storeName)) {
      props.put(KafkaPushJob.VENICE_STORE_NAME_PROP, storeName);
    }

    String veniceUrl = conf.get(KafkaPushJob.VENICE_URL_PROP);
    if(!Utils.isNullOrEmpty(veniceUrl)) {
      props.put(KafkaPushJob.VENICE_URL_PROP,veniceUrl);
    }
    return props;

  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
    // TODO Auto-generated method stub

  }
}
