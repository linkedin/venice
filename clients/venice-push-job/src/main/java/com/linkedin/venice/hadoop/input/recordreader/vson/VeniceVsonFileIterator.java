package com.linkedin.venice.hadoop.input.recordreader.vson;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceVsonFileIterator implements VeniceRecordIterator {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVsonRecordReader.class);

  private SequenceFile.Reader fileReader;
  private final VeniceVsonRecordReader recordReader;

  private final BytesWritable currentKey = new BytesWritable();
  private final BytesWritable currentValue = new BytesWritable();

  public VeniceVsonFileIterator(FileSystem fileSystem, Path hdfsPath, VeniceVsonRecordReader recordReader) {
    if (fileSystem == null) {
      LOGGER.error("FileSystem cannot be null for VeniceVsonFileIterator");
      throw new VeniceException("FileSystem cannot be null for VeniceVsonFileIterator");
    }
    if (hdfsPath == null) {
      LOGGER.error("Path cannot be null for VeniceVsonFileIterator");
      throw new VeniceException("Path cannot be null for VeniceVsonFileIterator");
    }
    if (recordReader == null) {
      LOGGER.error("RecordReader cannot be null for VeniceVsonFileIterator");
      throw new VeniceException("RecordReader cannot be null for VeniceVsonFileIterator");
    }

    try {
      this.fileReader = new SequenceFile.Reader(fileSystem, hdfsPath, new Configuration());
    } catch (IOException e) {
      String errorMessage =
          String.format("Failed to open file: %s. Ensure that the file is a valid sequence file.", hdfsPath.getName());
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }

    this.recordReader = recordReader;
  }

  @Override
  public byte[] getCurrentKey() {
    return recordReader.getKeyBytes(currentKey, currentValue);
  }

  @Override
  public byte[] getCurrentValue() {
    return recordReader.getValueBytes(currentKey, currentValue);
  }

  @Override
  public boolean next() {
    try {
      return fileReader.next(currentKey, currentValue);
    } catch (IOException e) {
      LOGGER.error("Error reading next record from file", e);
      throw new VeniceException(e);
    }
  }

  @Override
  public void close() throws IOException {
    Utils.closeQuietlyWithErrorLogged(fileReader);
  }
}
