package com.linkedin.venice.spark.input.hdfs;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.InputPartition;


public class VeniceHdfsInputPartition implements InputPartition {
  private static final long serialVersionUID = 1L;

  private transient Path filePath;
  // Path was not serializable till HDFS version 3.0.0, so we use URI instead:
  // https://issues.apache.org/jira/browse/HADOOP-13519
  private final URI filePathURI;

  public VeniceHdfsInputPartition(Path filePath) {
    this.filePath = filePath;
    this.filePathURI = filePath.toUri();
  }

  public Path getFilePath() {
    // Transient fields are not serialized, so we need to reinitialize them after deserialization
    if (filePath == null) {
      filePath = new Path(filePathURI);
    }
    return filePath;
  }
}
