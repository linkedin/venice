package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class reads the value field given the key field from an avro file based on a schema
 */
public class GenericAvroRecordReader {
  private static final Logger LOGGER = LogManager.getLogger(GenericAvroRecordReader.class);
  private final InputStream inputStream;
  private final DataFileStream avroDataFileStream;
  private final Schema fileSchema;
  private final Object avroObject;

  public GenericAvroRecordReader(FileSystem fs, String file) {
    if (fs != null && file != null) {
      try {
        inputStream = fs.open(new Path(file));
        avroDataFileStream = new DataFileStream(inputStream, new GenericDatumReader());
        fileSchema = avroDataFileStream.getSchema();
        try {
          avroObject = avroDataFileStream.next();
        } catch (NoSuchElementException e) {
          throw new VeniceException("File " + file + " contains no records", e);
        }
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Avro data from " + file
                + ". Check if the file exists and the data is in Avro format.",
            e);
      }
    } else {
      throw new VeniceException("Both fs and file should not be null");
    }
  }

  public Object getField(String key) {
    // Find the key in schema
    Schema.Field keyField = fileSchema.getField(key);
    if (keyField == null) {
      throw new VeniceSchemaFieldNotFoundException(key, "Could not find field: " + key + " from " + fileSchema);
    }

    // Find key's position
    int keyFieldPos = keyField.pos();

    // Find the value field
    try {
      AvroWrapper<IndexedRecord> avroKey = new AvroWrapper<>((IndexedRecord) avroObject);
      return getAvroValue(avroKey, keyFieldPos);
    } catch (VeniceException e) {
      LOGGER.error("Failed to get value for key: {}", key, e);
    }
    return null;
  }

  private Object getAvroValue(AvroWrapper<IndexedRecord> record, int keyFieldPos) {
    return record.datum().get(keyFieldPos);
  }

  public void close() {
    Utils.closeQuietlyWithErrorLogged(avroDataFileStream);
    Utils.closeQuietlyWithErrorLogged(inputStream);
  }
}
