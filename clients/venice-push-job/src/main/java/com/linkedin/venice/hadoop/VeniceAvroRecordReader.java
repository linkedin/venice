package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.hadoop.VenicePushJob.EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SCHEMA_STRING_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.etl.ETLUtils;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAvroRecordReader extends AbstractVeniceRecordReader<AvroWrapper<IndexedRecord>, NullWritable> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAvroRecordReader.class);

  private int keyFieldPos;
  private int valueFieldPos;

  private InputStream hdfsInputStream;
  private DataFileStream avroDataFileStream;

  private Schema storeSchema;
  private Schema fileSchema;

  private final ETLValueSchemaTransformation etlValueSchemaTransformation;

  /**
   * This constructor is used when data is read from HDFS.
   * @param topicName Topic which is to be published to
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param fs File system where the source data exists
   * @param hdfsPath Path of the avro file in the File system
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public VeniceAvroRecordReader(
      String topicName,
      String keyFieldStr,
      String valueFieldStr,
      FileSystem fs,
      Path hdfsPath,
      ETLValueSchemaTransformation etlValueSchemaTransformation) {
    super(topicName);
    if (fs != null && hdfsPath != null) {
      try {
        this.hdfsInputStream = fs.open(hdfsPath);
        avroDataFileStream = new DataFileStream(hdfsInputStream, new GenericDatumReader());
        fileSchema = avroDataFileStream.getSchema();
      } catch (IOException e) {
        throw new VeniceException(
            "Encountered exception reading Avro data from " + hdfsPath
                + ". Check if the file exists and the data is in Avro format.",
            e);
      }
    }
    this.etlValueSchemaTransformation = etlValueSchemaTransformation;
    setupSchema(keyFieldStr, valueFieldStr);
  }

  public VeniceAvroRecordReader(VeniceProperties props) {
    this(
        props.getString(TOPIC_PROP),
        AvroSchemaParseUtils.parseSchemaFromJSON(
            props.getString(SCHEMA_STRING_PROP),
            props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED)),
        props.getString(KEY_FIELD_PROP),
        props.getString(VALUE_FIELD_PROP),
        ETLValueSchemaTransformation
            .valueOf(props.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name())));
  }

  /**
   * This constructor is used in the Mapper.
   * @param topicName Topic which is to be published to
   * @param fileSchema Schema of the source files
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public VeniceAvroRecordReader(
      String topicName,
      Schema fileSchema,
      String keyFieldStr,
      String valueFieldStr,
      ETLValueSchemaTransformation etlValueSchemaTransformation) {
    super(topicName);
    this.fileSchema = fileSchema;
    this.etlValueSchemaTransformation = etlValueSchemaTransformation;
    setupSchema(keyFieldStr, valueFieldStr);
  }

  private void setupSchema(String keyFieldStr, String valueFieldStr) {
    Schema.Field keyField = fileSchema.getField(keyFieldStr);
    Schema.Field valueField = fileSchema.getField(valueFieldStr);

    if (keyField == null) {
      throw new VeniceSchemaFieldNotFoundException(
          keyFieldStr,
          "Could not find field: " + keyFieldStr + " from " + fileSchema.toString());
    }

    if (valueField == null) {
      throw new VeniceSchemaFieldNotFoundException(
          valueFieldStr,
          "Could not find field: " + valueFieldStr + " from " + fileSchema.toString());
    }

    if (!etlValueSchemaTransformation.equals(ETLValueSchemaTransformation.NONE)) {
      List<Schema.Field> storeSchemaFields = new LinkedList<>();

      for (Schema.Field fileField: fileSchema.getFields()) {
        Schema fieldSchema = fileField.schema();
        // In our ETL jobs, when we see a "delete" record, we set the value as "null" and set the DELETED_TS to the
        // timestamp when this record was deleted. To allow the value field to be set as "null", we make the schema of
        // the value field as a union schema of "null" and the original value schema. To push back to Venice from ETL
        // data, we strip the schema of the value field of the union type, leaving just the original value schema thus
        // passing the schema validation.
        if (fileField.name().equals(valueFieldStr)) {
          fieldSchema = ETLUtils.getValueSchemaFromETLValueSchema(fieldSchema, etlValueSchemaTransformation);
        }
        storeSchemaFields.add(AvroCompatibilityHelper.newField(fileField).setSchema(fieldSchema).build());
      }

      storeSchema = Schema
          .createRecord(fileSchema.getName(), fileSchema.getDoc(), fileSchema.getNamespace(), fileSchema.isError());
      storeSchema.setFields(storeSchemaFields);
    } else {
      storeSchema = fileSchema;
    }

    Schema.Field storeKeyField = storeSchema.getField(keyFieldStr);
    Schema.Field storeValueField = storeSchema.getField(valueFieldStr);

    keyFieldPos = storeKeyField.pos();
    valueFieldPos = storeValueField.pos();

    configure(storeKeyField.schema().toString(), storeValueField.schema().toString());
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
    return record.datum().get(valueFieldPos);
  }

  public Schema getFileSchema() {
    return fileSchema;
  }

  public Schema getStoreSchema() {
    return storeSchema;
  }

  @Override
  public Iterator<Pair<byte[], byte[]>> iterator() {
    if (avroDataFileStream == null) {
      LOGGER.warn("Data not iterable due to incorrect file information.");
      return Collections.emptyIterator();
    }

    return new AvroIterator(avroDataFileStream, topicName, this);
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(avroDataFileStream);
    Utils.closeQuietlyWithErrorLogged(hdfsInputStream);
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
        byte[] keyBytes =
            recordReader.getKeySerializer().serialize(topic, recordReader.getAvroKey(hadoopKey, hadoopValue));
        Object avroValue = recordReader.getAvroValue(hadoopKey, hadoopValue);
        byte[] valueBytes = null;
        if (avroValue != null) {
          valueBytes = recordReader.getValueSerializer().serialize(topic, avroValue);
        }
        return Pair.create(keyBytes, valueBytes);
      } catch (VeniceException e) {
        LOGGER.error("Failed to get next record", e);
      }
      return null;
    }
  }
}
