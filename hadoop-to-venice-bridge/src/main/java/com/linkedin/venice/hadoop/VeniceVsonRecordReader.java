package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.utils.Pair;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;


public class VeniceVsonRecordReader extends AbstractVeniceRecordReader<BytesWritable, BytesWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceVsonRecordReader.class);

  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private String keyField;
  private String valueField;

  private TreeMap<String, String> metadataMap;

  private SequenceFile.Reader fileReader;

  public VeniceVsonRecordReader(String topicName, String keySchemaString, String valueSchemaString, String keyField, String valueField) {
    super(topicName);
    this.keyField = keyField;
    this.valueField = valueField;
    setupSchema(keySchemaString, valueSchemaString);
  }

  public VeniceVsonRecordReader(String topicName, String keyField, String valueField, FileSystem fs, Path hdfsPath) {
    super(topicName);
    this.keyField = keyField;
    this.valueField = valueField;
    metadataMap = new TreeMap<>();

    if (fs != null && hdfsPath != null) {
      try {
        fileReader = new SequenceFile.Reader(fs, hdfsPath, new Configuration());
        fileReader.getMetadata().getMetadata().forEach((key, value) -> metadataMap.put(key.toString(), value.toString()));
        setupSchema(metadataMap.get(FILE_KEY_SCHEMA), metadataMap.get(FILE_VALUE_SCHEMA));
      } catch (IOException e) {
        LOGGER.info("Path: " + hdfsPath.getName() + " is not a sequence file.");
      }
    }
  }

  private void setupSchema(String keySchemaString, String valueSchemaString) {
    keyDeserializer = VsonAvroSerializer.fromSchemaStr(keySchemaString);
    valueDeserializer = VsonAvroSerializer.fromSchemaStr(valueSchemaString);

    String keyAvroSchemaStr = getFieldSchema(keySchemaString, keyField);
    String valueAvroSchemaStr = getFieldSchema(valueSchemaString, valueField);

    configure(keyAvroSchemaStr, valueAvroSchemaStr);
  }

  private String getFieldSchema(String schemaString, String field) {
    if (field.isEmpty()) {
      return VsonAvroSchemaAdapter.parse(schemaString).toString();
    } else {
      Schema.Field keySchemaField = VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(schemaString))
          .getField(field);
      if (keySchemaField == null) {
        throw new VeniceSchemaFieldNotFoundException(field, "Could not find field: " + field + " from " + schemaString);
      }
      return keySchemaField.schema().toString();
    }
  }

  public VsonAvroSerializer getKeyDeserializer() {
    return keyDeserializer;
  }

  public VsonAvroSerializer getValueDeserializer() {
    return valueDeserializer;
  }

  @Override
  protected Object getAvroKey(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroKeyObject = keyDeserializer.bytesToAvro(inputKey.getBytes());
    if (!keyField.isEmpty()) {
      return ((GenericData.Record) avroKeyObject).get(keyField);
    }
    return avroKeyObject;
  }

  @Override
  protected Object getAvroValue(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroValueObject = valueDeserializer.bytesToAvro(inputValue.getBytes());
    if (!valueField.isEmpty()) {
      return ((GenericData.Record) avroValueObject).get(valueField);
    }
    return avroValueObject;
  }

  public String getKeyField() {
    return keyField;
  }

  public String getValueField() {
    return valueField;
  }

  public Map<String, String> getMetadataMap() {
    return metadataMap;
  }

  @NotNull
  @Override
  public Iterator<Pair<byte[], byte[]>> iterator() {
    if (fileReader == null) {
      LOGGER.warn("Data not iterable due to incorrect file information.");
      return Collections.emptyIterator();
    }

    return new VsonIterator(fileReader, topicName, this);
  }

  @Override
  public void close() throws IOException {
    fileReader.close();
  }

  private static class VsonIterator implements Iterator<Pair<byte[], byte[]>> {
    SequenceFile.Reader fileReader;
    private String topic;
    private VeniceVsonRecordReader recordReader;

    public VsonIterator(SequenceFile.Reader fileReader, String topic, VeniceVsonRecordReader recordReader) {
      this.fileReader = fileReader;
      this.topic = topic;
      this.recordReader = recordReader;
    }

    @Override
    public boolean hasNext() {
      try {
        long position = fileReader.getPosition();
        // TODO: Reuse objects for higher GC efficiency.
        BytesWritable key = (BytesWritable) fileReader.getKeyClass().newInstance();
        BytesWritable value = (BytesWritable) fileReader.getValueClass().newInstance();
        boolean hasNext = fileReader.next(key, value);
        fileReader.seek(position);
        return hasNext;
      } catch (IOException e) {
        throw new VeniceException("Encountered exception reading Vson data. Check if "
            + "the file exists and the data is in Avro format.", e);
      } catch (IllegalAccessException e) {
        LOGGER.info("Unable to access class constructor through reflection. Exception: " + e.toString());
      } catch (InstantiationException e) {
        LOGGER.info("Class cannot be instantiated. Exception: " + e.toString());
      }
      return false;
    }

    @Override
    public Pair<byte[], byte[]> next() {
      try {
        BytesWritable key = (BytesWritable) fileReader.getKeyClass().newInstance();
        BytesWritable value = (BytesWritable) fileReader.getValueClass().newInstance();
        boolean hasNext = fileReader.next(key, value);
        if (hasNext) {
          Object avroKey = recordReader.getKeyDeserializer().bytesToAvro(key.getBytes());
          Object avroValue = recordReader.getValueDeserializer().bytesToAvro(value.getBytes());
          if (!recordReader.getKeyField().isEmpty()) {
            avroKey = ((GenericData.Record) avroKey).get(recordReader.getKeyField());
          }
          if (!recordReader.getValueField().isEmpty()) {
            avroValue = ((GenericData.Record) avroValue).get(recordReader.getValueField());
          }
          byte[] keyBytes = recordReader.getKeySerializer().serialize(topic, avroKey);
          byte[] valueBytes = recordReader.getValueSerializer().serialize(topic, avroValue);
          return Pair.create(keyBytes, valueBytes);
        }
      } catch (IllegalAccessException e) {
        LOGGER.info("Unable to access class constructor through reflection. Exception: " + e.toString());
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        LOGGER.info("Class cannot be instantiated. Exception: " + e.toString());
      }
      return null;
    }
  }
}
