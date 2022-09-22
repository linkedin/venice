package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.FILE_KEY_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJob.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceVsonRecordReader extends AbstractVeniceRecordReader<BytesWritable, BytesWritable> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVsonRecordReader.class);

  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private final String keyField;
  private final String valueField;

  private TreeMap<String, String> metadataMap;

  private SequenceFile.Reader fileReader;

  public VeniceVsonRecordReader(VeniceProperties props) {
    this(
        props.getString(TOPIC_PROP),
        props.getString(FILE_KEY_SCHEMA),
        props.getString(FILE_VALUE_SCHEMA),
        props.getString(KEY_FIELD_PROP, ""),
        props.getString(VALUE_FIELD_PROP, ""));
  }

  public VeniceVsonRecordReader(
      String topicName,
      String keySchemaString,
      String valueSchemaString,
      String keyField,
      String valueField) {
    super(topicName);
    this.keyField = keyField == null ? "" : keyField;
    this.valueField = valueField == null ? "" : valueField;
    setupSchema(keySchemaString, valueSchemaString);
  }

  public VeniceVsonRecordReader(String topicName, String keyField, String valueField, FileSystem fs, Path hdfsPath) {
    super(topicName);
    this.keyField = keyField == null ? "" : keyField;
    this.valueField = valueField == null ? "" : valueField;
    metadataMap = new TreeMap<>();

    if (fs != null && hdfsPath != null) {
      try {
        fileReader = new SequenceFile.Reader(fs, hdfsPath, new Configuration());
        fileReader.getMetadata()
            .getMetadata()
            .forEach((key, value) -> metadataMap.put(key.toString(), value.toString()));
        setupSchema(metadataMap.get(FILE_KEY_SCHEMA), metadataMap.get(FILE_VALUE_SCHEMA));
      } catch (IOException e) {
        LOGGER.info("Path: {} is not a sequence file.", hdfsPath.getName());
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
      Schema.Field keySchemaField =
          VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(schemaString)).getField(field);
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
    Object avroKeyObject = keyDeserializer.bytesToAvro(inputKey.getBytes(), 0, inputKey.getLength());
    if (!keyField.isEmpty()) {
      return ((GenericData.Record) avroKeyObject).get(keyField);
    }
    return avroKeyObject;
  }

  @Override
  protected Object getAvroValue(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroValueObject = valueDeserializer.bytesToAvro(inputValue.getBytes(), 0, inputValue.getLength());
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

  @Override
  public Iterator<Pair<byte[], byte[]>> iterator() {
    if (fileReader == null) {
      LOGGER.warn("Data not iterable due to incorrect file information.");
      return Collections.emptyIterator();
    }

    return new VsonIterator(fileReader, topicName, this);
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(fileReader);
  }

  private static class VsonIterator implements Iterator<Pair<byte[], byte[]>> {
    SequenceFile.Reader fileReader;
    private String topic;
    private VeniceVsonRecordReader recordReader;
    private BytesWritable currentKey = null;
    private BytesWritable currentValue = null;
    private boolean currentValueRead = true;
    private boolean hasNext;

    public VsonIterator(SequenceFile.Reader fileReader, String topic, VeniceVsonRecordReader recordReader) {
      this.fileReader = fileReader;
      this.topic = topic;
      this.recordReader = recordReader;
      try {
        currentKey = (BytesWritable) fileReader.getKeyClass().newInstance();
        currentValue = (BytesWritable) fileReader.getValueClass().newInstance();
      } catch (IllegalAccessException e) {
        LOGGER.warn("Unable to access class constructor through reflection. Exception: {}", e.toString());
      } catch (InstantiationException e) {
        LOGGER.warn("Class cannot be instantiated. Exception: {}", e.toString());
      }
    }

    @Override
    public boolean hasNext() {
      if (!currentValueRead) {
        return hasNext;
      }
      try {
        hasNext = fileReader.next(currentKey, currentValue);
        currentValueRead = false;
        return hasNext;
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Vson data. Check if the file exists and the data is in Vson format.",
            e);
      }
    }

    @Override
    public Pair<byte[], byte[]> next() {
      currentValueRead = true;
      Object avroKey = recordReader.getKeyDeserializer().bytesToAvro(currentKey.getBytes(), 0, currentKey.getLength());
      Object avroValue =
          recordReader.getValueDeserializer().bytesToAvro(currentValue.getBytes(), 0, currentValue.getLength());
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
  }
}
