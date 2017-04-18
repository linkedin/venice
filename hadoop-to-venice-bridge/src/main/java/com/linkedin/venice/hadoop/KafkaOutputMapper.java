package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import javafx.util.Pair;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class KafkaOutputMapper implements Mapper<AvroWrapper<IndexedRecord>, NullWritable, NullWritable, NullWritable> {
  private VeniceMapper veniceMapper = null;
  private VeniceReducer veniceReducer = null;

  @Override
  public void map(AvroWrapper<IndexedRecord> record, NullWritable value, OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
    if (null == veniceMapper || null == veniceReducer) {
      throw new VeniceException("KafkaOutputMapper is not initialized yet");
    }
    Pair<byte[], byte[]> keyValuePair = veniceMapper.parseAvroRecord(record);
    /**
     * Skip record with null value, check {@link VeniceMapper#parseAvroRecord(AvroWrapper)}.
     */
    if (null != keyValuePair) {
      veniceReducer.sendMessageToKafka(keyValuePair.getKey(), keyValuePair.getValue(), reporter);
    }
  }

  @Override
  public void close() throws IOException {
    if (null != veniceMapper) {
      veniceMapper.close();
    }
    if (null != veniceReducer) {
      veniceReducer.close();
    }
  }

  @Override
  public void configure(JobConf job) {
    veniceMapper = new VeniceMapper();
    veniceMapper.configure(job);
    veniceReducer = new VeniceReducer();
    veniceReducer.configure(job);
  }
}
