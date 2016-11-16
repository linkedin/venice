package com.linkedin.venice.hadoop.pbnj;

import com.linkedin.venice.hadoop.utils.HadoopUtils;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * An {@link OutputFormat} implementation which instantiates and configures an
 * {@link PostBulkloadAnalysisRecordWriter} in order to query Venice Routers and
 * confirm that all records have been bulk loaded properly in Venice.
 */
public class PostBulkloadAnalysisOutputFormat implements OutputFormat<AvroWrapper<IndexedRecord>, NullWritable> {
  private static final Logger LOGGER = Logger.getLogger(PostBulkloadAnalysisOutputFormat.class);

  public PostBulkloadAnalysisOutputFormat() {
    super();
    LOGGER.info(this.getClass().getSimpleName() + " constructed.");
  }

  @Override
  public RecordWriter<AvroWrapper<IndexedRecord>, NullWritable> getRecordWriter(FileSystem fileSystem,
                                                                                JobConf conf,
                                                                                String arg2,
                                                                                Progressable progress) throws IOException {
    LOGGER.info("getRecordWriter called.");
    return new PostBulkloadAnalysisRecordWriter(HadoopUtils.getVeniceProps(conf), progress);
  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
    // TODO Auto-generated method stub

  }
}
