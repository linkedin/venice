package com.linkedin.venice.hadoop.input.recordreader.vson;

import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;

import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
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


/**
 * A record reader that reads records from VSON SequenceFile file input into Avro-serialized keys and values.
 */
public class VeniceVsonRecordReader extends AbstractVeniceRecordReader<BytesWritable, BytesWritable> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVsonRecordReader.class);

  private VsonAvroSerializer keyDeserializer;
  private VsonAvroSerializer valueDeserializer;

  private final String keyField;
  private final String valueField;

  private TreeMap<String, String> metadataMap;

  public VeniceVsonRecordReader(VeniceProperties props) {
    this(
        props.getString(FILE_KEY_SCHEMA),
        props.getString(FILE_VALUE_SCHEMA),
        props.getString(KEY_FIELD_PROP, ""),
        props.getString(VALUE_FIELD_PROP, ""));
  }

  public VeniceVsonRecordReader(String keySchemaString, String valueSchemaString, String keyField, String valueField) {
    this.keyField = keyField == null ? "" : keyField;
    this.valueField = valueField == null ? "" : valueField;
    setupSchema(keySchemaString, valueSchemaString);
  }

  public VeniceVsonRecordReader(String keyField, String valueField, FileSystem fs, Path hdfsPath) {
    this.keyField = keyField == null ? "" : keyField;
    this.valueField = valueField == null ? "" : valueField;
    metadataMap = new TreeMap<>();

    if (fs != null && hdfsPath != null) {
      try (SequenceFile.Reader fileReader = new SequenceFile.Reader(fs, hdfsPath, new Configuration())) {
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
    valueDeserializer = VsonAvroSerializer.fromSchemaStr(valueSchemaString);
    keyDeserializer = VsonAvroSerializer.fromSchemaStr(keySchemaString);

    configure(getFieldSchema(keySchemaString, keyField), getFieldSchema(valueSchemaString, valueField));
  }

  @Override
  public Object getAvroKey(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroKeyObject = keyDeserializer.bytesToAvro(inputKey.getBytes(), 0, inputKey.getLength());
    if (!keyField.isEmpty()) {
      return ((GenericData.Record) avroKeyObject).get(keyField);
    }
    return avroKeyObject;
  }

  @Override
  public Object getAvroValue(BytesWritable inputKey, BytesWritable inputValue) {
    Object avroValueObject = valueDeserializer.bytesToAvro(inputValue.getBytes(), 0, inputValue.getLength());
    if (!valueField.isEmpty()) {
      return ((GenericData.Record) avroValueObject).get(valueField);
    }
    return avroValueObject;
  }

  public Map<String, String> getMetadataMap() {
    return metadataMap;
  }

  private static Schema getFieldSchema(String schemaString, String field) {
    if (field == null || field.isEmpty()) {
      return VsonAvroSchemaAdapter.parse(schemaString);
    } else {
      Schema.Field keySchemaField =
          VsonAvroSchemaAdapter.stripFromUnion(VsonAvroSchemaAdapter.parse(schemaString)).getField(field);
      if (keySchemaField == null) {
        throw new VeniceSchemaFieldNotFoundException(field, "Could not find field: " + field + " from " + schemaString);
      }
      return keySchemaField.schema();
    }
  }
}
