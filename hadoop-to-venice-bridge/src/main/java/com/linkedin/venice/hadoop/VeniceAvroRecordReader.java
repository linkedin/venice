package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.utils.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

public class VeniceAvroRecordReader extends AbstractVeniceRecordReader<AvroWrapper<IndexedRecord>, NullWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceAvroRecordReader.class);

  private int keyFieldPos;
  private int valueFieldPos;

  private InputStream hdfsInputStream;
  private DataFileStream avroDataFileStream;

  private Schema schema;

  public VeniceAvroRecordReader(String topicName, String keyFieldStr, String valueFieldStr, FileSystem fs, Path hdfsPath) {
    super(topicName);
    if (fs != null && hdfsPath != null) {
      try {
        this.hdfsInputStream = fs.open(hdfsPath);
        avroDataFileStream = new DataFileStream(hdfsInputStream, new GenericDatumReader());
        schema = avroDataFileStream.getSchema();
      } catch (IOException e) {
        throw new VeniceException("Encountered exception reading Avro data from " + hdfsPath.toString() + ". Check if "
            + "the file exists and the data is in Avro format.", e);
      }
    }

    setupSchema(keyFieldStr, valueFieldStr);
  }

  public VeniceAvroRecordReader(String topicName, Schema schema, String keyFieldStr, String valueFieldStr) {
    super(topicName);
    this.schema = schema;
    setupSchema(keyFieldStr, valueFieldStr);
  }

  private void setupSchema(String keyFieldStr, String valueFieldStr) {
    Schema.Field keyField = schema.getField(keyFieldStr);
    Schema.Field valueField = schema.getField(valueFieldStr);

    if (keyField == null) {
      throw new VeniceSchemaFieldNotFoundException(keyFieldStr, "Could not find field: " + keyFieldStr + " from " + schema.toString());
    }

    if (valueField == null) {
      throw new VeniceSchemaFieldNotFoundException(valueFieldStr, "Could not find field: " + valueFieldStr + " from " + schema.toString());
    }

    String keySchemaStr = keyField.schema().toString();
    keyFieldPos = keyField.pos();

    String valueSchemaStr = valueField.schema().toString();
    valueFieldPos = valueField.pos();

    configure(keySchemaStr, valueSchemaStr);
  }

  @Override
  protected Object getAvroKey(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    Object keyDatum = record.datum().get(keyFieldPos);

    if (keyDatum == null) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    return keyDatum;
  }

  @Override
  protected Object getAvroValue(AvroWrapper<IndexedRecord> record, NullWritable nullValue) {
    Object valueDatum = record.datum().get(valueFieldPos);

    if (null == valueDatum) {
      // TODO: maybe we want to write a null value anyways?
      LOGGER.warn("Skipping record with null value.");
      return null;
    }

    return valueDatum;
  }

  public Schema getSchema() {
    return schema;
  }

  @NotNull
  @Override
  public Iterator<Pair<byte[], byte[]>> iterator() {
    if (avroDataFileStream == null) {
      LOGGER.warn("Data not iterable due to incorrect file information.");
      return Collections.emptyIterator();
    }

    return new AvroIterator(avroDataFileStream, topicName, this);
  }

  @Override
  public void close() throws IOException {
    avroDataFileStream.close();
    hdfsInputStream.close();
  }

  private static class AvroIterator implements Iterator<Pair<byte[], byte[]>> {
    private DataFileStream avroDataFileStream;
    private String topic;
    private VeniceAvroRecordReader recordReader;

    public AvroIterator(DataFileStream avroDataFileStream, String topic, VeniceAvroRecordReader recordReader) {
      this.avroDataFileStream = avroDataFileStream;
      this.topic = topic;
      this.recordReader = recordReader;
    }

    @Override
    public boolean hasNext() {
      return avroDataFileStream.hasNext();
    }

    @Override
    public Pair<byte[], byte[]> next() {
      Object avroObject = avroDataFileStream.next();
      try {
        AvroWrapper<IndexedRecord> hadoopKey = new AvroWrapper<>((IndexedRecord) avroObject);
        NullWritable hadoopValue = NullWritable.get();
        byte[] keyBytes = recordReader.getKeySerializer().serialize(topic, recordReader.getAvroKey(hadoopKey, hadoopValue));
        byte[] valueBytes = recordReader.getValueSerializer().serialize(topic, recordReader.getAvroValue(hadoopKey, hadoopValue));
        return Pair.create(keyBytes, valueBytes);
      } catch (VeniceException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}
