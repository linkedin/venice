package com.linkedin.venice.hadoop;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * A Hadoop Mapper implementation which collects simple stats and debugging info
 * about the records going through it.
 */
public class KafkaOutputMapper extends AvroMapper<IndexedRecord, IndexedRecord> {
  private static final int COUNTER_STATEMENT_COUNT = 100000;
  private long counter;
  private long lastTimeChecked = System.currentTimeMillis();
  private static Logger logger = Logger.getLogger(KafkaPushJob.class);

  public void map(IndexedRecord record, AvroCollector<IndexedRecord> output, Reporter reporter) throws IOException {

    // Calls AvroKafkaRecordWriter.write().
    if (counter == 0L) {
      logger.info("Printing first record's schema:");
      logger.info(record.getSchema().toString(true));
    }

    output.collect(record);
    counter++;
    if (counter % COUNTER_STATEMENT_COUNT == 0) {
      double transferRate = COUNTER_STATEMENT_COUNT * 1000 / (System.currentTimeMillis() - lastTimeChecked);
      logger.info("Record count: " + counter + " rec/s:" + transferRate);
      lastTimeChecked = System.currentTimeMillis();
    }

    reporter.incrCounter("Kafka", "Output Records", 1);
  }
}

