package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;


public class VeniceAvroFileIterator implements VeniceRecordIterator {
  private final InputStream hdfsInputStream;
  private final DataFileStream avroDataFileStream;
  private final AbstractAvroRecordReader<AvroWrapper<IndexedRecord>, NullWritable> recordReader;

  private byte[] currentKey = null;
  private byte[] currentValue = null;
  private byte[] currentRmd = null;

  public VeniceAvroFileIterator(
      FileSystem fs,
      Path hdfsPath,
      AbstractAvroRecordReader<AvroWrapper<IndexedRecord>, NullWritable> recordReader) {
    if (fs != null && hdfsPath != null) {
      try {
        this.hdfsInputStream = fs.open(hdfsPath);
        avroDataFileStream = new DataFileStream(hdfsInputStream, new GenericDatumReader());
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Avro data from " + hdfsPath
                + ". Check if the file exists and the data is in Avro format.",
            e);
      }
    } else {
      throw new VeniceException("Invalid file system or path");
    }
    this.recordReader = recordReader;
  }

  @Override
  public byte[] getCurrentKey() {
    return currentKey;
  }

  @Override
  public byte[] getCurrentValue() {
    return currentValue;
  }

  @Override
  public byte[] getCurrentRmd() {
    return currentRmd;
  }

  @Override
  public boolean next() {
    if (!avroDataFileStream.hasNext()) {
      return false;
    }

    AvroWrapper<IndexedRecord> avroObject = new AvroWrapper<>((IndexedRecord) avroDataFileStream.next());
    currentKey = recordReader.getKeyBytes(avroObject, null);
    currentValue = recordReader.getValueBytes(avroObject, null);
    currentRmd = recordReader.getRmdBytes(avroObject, null);
    return true;
  }

  @Override
  public void close() throws IOException {
    Utils.closeQuietlyWithErrorLogged(avroDataFileStream);
    Utils.closeQuietlyWithErrorLogged(hdfsInputStream);
  }
}
