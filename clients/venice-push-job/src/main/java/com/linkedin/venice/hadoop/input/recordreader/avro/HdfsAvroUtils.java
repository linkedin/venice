package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public final class HdfsAvroUtils {
  private HdfsAvroUtils() {
  }

  public static Schema getFileSchema(FileSystem fs, Path path) {
    if (fs != null && path != null) {
      try (FSDataInputStream hdfsInputStream = fs.open(path);
          DataFileStream avroDataFileStream = new DataFileStream(hdfsInputStream, new GenericDatumReader())) {
        return avroDataFileStream.getSchema();
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Avro data from " + path
                + ". Check if the file exists and the data is in Avro format.",
            e);
      }
    } else {
      throw new VeniceException("Invalid file system or path");
    }
  }
}
