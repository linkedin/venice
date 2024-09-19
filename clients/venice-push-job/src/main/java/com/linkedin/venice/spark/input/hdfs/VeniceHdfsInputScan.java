package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class VeniceHdfsInputScan implements Scan, Batch {
  private final VeniceProperties jobConfig;

  public VeniceHdfsInputScan(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    try {
      Path inputDirPath = new Path(jobConfig.getString(INPUT_PATH_PROP));
      FileSystem fs = inputDirPath.getFileSystem(new Configuration());
      List<VeniceHdfsInputPartition> inputPartitionList = new ArrayList<>();
      // For now, we create 1 file as 1 InputPartition. This is not the most ideal, because Avro allows splitting files
      // to a smaller granularity using sync markers. We can explore later if we feel we need that optimization.
      RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(inputDirPath, false);
      while (fileStatusIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusIterator.next();
        Path filePath = fileStatus.getPath();
        if (PATH_FILTER.accept(filePath)) {
          inputPartitionList.add(new VeniceHdfsInputPartition(fileStatus.getPath()));
        }
      }
      return inputPartitionList.toArray(new VeniceHdfsInputPartition[0]);
    } catch (IOException e) {
      throw new VeniceException("Could not get FileSystem", e);
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new VeniceHdfsInputPartitionReaderFactory(jobConfig);
  }

  @Override
  public StructType readSchema() {
    return SparkConstants.DEFAULT_SCHEMA;
  }

  @Override
  public Batch toBatch() {
    return this;
  }
}
