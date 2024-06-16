package com.linkedin.venice.hadoop.spark.datawriter.recordreader;

import static com.linkedin.venice.hadoop.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.hadoop.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.input.recordreader.avro.AbstractAvroRecordReader;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;


/**
 * This class is used to extract value records from the specified Dali input. The key is dummy and should not be used.
 * The input is the deserialized Avro object on which the {@link ETLValueSchemaTransformation} needs to be applied.
 */
public class VeniceSparkValueRecordReader extends AbstractAvroRecordReader<Void, byte[]> {
  private static final ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[0]);
  private final RecordDeserializer<Object> deserializer;

  public VeniceSparkValueRecordReader(Schema valueSchema, ETLValueSchemaTransformation etlValueSchemaTransformation) {
    super(createDataSchema(valueSchema), KEY_COLUMN_NAME, VALUE_COLUMN_NAME, etlValueSchemaTransformation, null);
    deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema);
  }

  @Override
  protected IndexedRecord getRecordDatum(Void ignored, byte[] value) {
    Object deserializedValue;
    if (value == null) {
      deserializedValue = null;
    } else {
      deserializedValue = deserializer.deserialize(value);
    }

    GenericRecord record = new GenericData.Record(getDataSchema());
    record.put(KEY_COLUMN_NAME, EMPTY_BYTES);
    record.put(VALUE_COLUMN_NAME, deserializedValue);
    return record;
  }

  /**
   * Create the schema for the data record that will be used to extract the value from ETL input.
   * @param valueSchema The schema of the value field.
   * @return The schema for the data record that will be used to extract the value from ETL input.
   */
  private static Schema createDataSchema(Schema valueSchema) {
    return Schema.createRecord(
        "DaliSparkSerializedRecord",
        null,
        "com.linkedin.venice.dali.spark",
        false,
        Arrays.asList(
            AvroCompatibilityHelper
                .createSchemaField(KEY_COLUMN_NAME, Schema.create(Schema.Type.BYTES), "A dummy field", null),
            AvroCompatibilityHelper
                .createSchemaField(VALUE_COLUMN_NAME, valueSchema, "The value field of record", null)));
  }
}
