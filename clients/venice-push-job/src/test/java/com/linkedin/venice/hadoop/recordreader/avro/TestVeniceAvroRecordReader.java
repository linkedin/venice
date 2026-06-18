package com.linkedin.venice.hadoop.recordreader.avro;

import static com.linkedin.venice.etl.ETLValueSchemaTransformation.ADD_NULL_TO_UNION;
import static com.linkedin.venice.etl.ETLValueSchemaTransformation.NONE;
import static com.linkedin.venice.etl.ETLValueSchemaTransformation.UNIONIZE_WITH_NULL;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.TestWriteUtils.INT_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_WITH_TIMESTAMP_RECORD_V1_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.WRITER_VALUE_SCHEMA_STRING_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVeniceAvroRecordReader {
  private static final RandomRecordGenerator RANDOM_RECORD_GENERATOR = new RandomRecordGenerator();
  private static final Object[] ETL_TRANSFORMATIONS = { NONE, ADD_NULL_TO_UNION, UNIONIZE_WITH_NULL };

  @DataProvider(name = "Boolean-and-EtlTransformations")
  public static Object[][] booleanAndEtlTransformations() {
    return DataProviderUtils.allPermutationGenerator(BOOLEAN, ETL_TRANSFORMATIONS);
  }

  @Test
  public void testGeneratePartialUpdate() {
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        STRING_TO_NAME_RECORD_V1_SCHEMA,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        updateSchema);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V1_SCHEMA);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(TestWriteUtils.NAME_RECORD_V1_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    record.put("value", valueRecord);
    Object result = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertTrue(result instanceof IndexedRecord);

    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("firstName").pos()), "FN");
    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("lastName").pos()), "LN");
    Assert.assertEquals(
        ((IndexedRecord) result).get(updateSchema.getField("age").pos()),
        new GenericData.Record(updateSchema.getField("age").schema().getTypes().get(0)));
  }

  @Test
  public void testGeneratePartialUpdateWithTimestamp() {
    Long timestamp = 123456789L;
    byte[] timestampBytes = String.valueOf(timestamp).getBytes();
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        STRING_TO_NAME_WITH_TIMESTAMP_RECORD_V1_SCHEMA,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        updateSchema);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_WITH_TIMESTAMP_RECORD_V1_SCHEMA);
    record.put("key", "123");
    record.put("rmd", timestampBytes);
    GenericRecord valueRecord = new GenericData.Record(TestWriteUtils.NAME_RECORD_V1_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    record.put("value", valueRecord);
    Object result = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertEquals(recordReader.getRmdValue(new AvroWrapper<>(record), NullWritable.get()), timestampBytes);
    Assert.assertTrue(result instanceof IndexedRecord);

    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("firstName").pos()), "FN");
    Assert.assertEquals(((IndexedRecord) result).get(updateSchema.getField("lastName").pos()), "LN");
    Assert.assertEquals(
        ((IndexedRecord) result).get(updateSchema.getField("age").pos()),
        new GenericData.Record(updateSchema.getField("age").schema().getTypes().get(0)));
  }

  @Test
  public void testGetAvroValueProjectsSupersetInputToWriterSchema() {
    // Input value schema (V2) is a strict superset of the writer (target) value schema (V1): V2 adds "age".
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        STRING_TO_NAME_RECORD_V2_SCHEMA,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        null,
        NAME_RECORD_V1_SCHEMA,
        null);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V2_SCHEMA);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    valueRecord.put("age", 42);
    record.put("value", valueRecord);

    Object result = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertTrue(result instanceof GenericRecord);
    GenericRecord projected = (GenericRecord) result;

    // Projected record conforms to the writer schema (V1): "age" is dropped, surviving fields retain their values.
    Assert.assertEquals(projected.getSchema(), NAME_RECORD_V1_SCHEMA);
    Assert.assertNull(projected.getSchema().getField("age"));
    Assert.assertEquals(projected.get("firstName").toString(), "FN");
    Assert.assertEquals(projected.get("lastName").toString(), "LN");
  }

  @Test
  public void testGetAvroValueReturnsNullForTombstoneWhenProjecting() {
    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        STRING_TO_NAME_RECORD_V2_SCHEMA,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        null,
        NAME_RECORD_V1_SCHEMA,
        null);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V2_SCHEMA);
    record.put("key", "123");
    record.put("value", null);

    Assert.assertNull(recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get()));
  }

  @Test
  public void testGetAvroValueWithoutWriterSchemaIsNotProjected() {
    // No writer value schema supplied: the reader must pass the input value through untouched.
    VeniceAvroRecordReader recordReader =
        new VeniceAvroRecordReader(STRING_TO_NAME_RECORD_V1_SCHEMA, "key", "value", DEFAULT_RMD_FIELD_PROP, NONE, null);

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V1_SCHEMA);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    record.put("value", valueRecord);

    Object result = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertSame(result, valueRecord);
  }

  @Test
  public void testGetRmdValueProjectsSupersetRmdToWriterRmdSchema() {
    // RMD generated against the superset value schema (V2) is projected down to the writer's RMD schema (V1).
    Schema writerValueSchema = NAME_RECORD_V1_SCHEMA;
    Schema inputRmdSchema = RmdSchemaGenerator.generateMetadataSchema(NAME_RECORD_V2_SCHEMA, 1);
    Schema writerRmdSchema = RmdSchemaGenerator.generateMetadataSchema(writerValueSchema, 1);

    Schema inputFileSchema = new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
        .setValueSchema(NAME_RECORD_V2_SCHEMA)
        .setFieldSchema(DEFAULT_RMD_FIELD_PROP, inputRmdSchema)
        .build();

    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        inputFileSchema,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        null,
        writerValueSchema,
        writerRmdSchema);

    GenericRecord record = new GenericData.Record(inputFileSchema);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    valueRecord.put("age", 42);
    record.put("value", valueRecord);

    GenericRecord rmdRecord = new GenericData.Record(inputRmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, 9999L);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>(Arrays.asList(1L, 2L)));
    record.put(DEFAULT_RMD_FIELD_PROP, rmdRecord);

    Object result = recordReader.getRmdValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertTrue(result instanceof GenericRecord);
    GenericRecord projected = (GenericRecord) result;

    Assert.assertEquals(projected.getSchema(), writerRmdSchema);
    Assert.assertEquals(projected.get(RmdConstants.TIMESTAMP_FIELD_NAME), 9999L);
    Assert.assertEquals(
        projected.get(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        new ArrayList<>(Arrays.asList(1L, 2L)));
  }

  @Test
  public void testGetRmdValueReturnsNullWhenRecordCarriesNoRmd() {
    Schema writerValueSchema = NAME_RECORD_V1_SCHEMA;
    Schema inputRmdSchema = RmdSchemaGenerator.generateMetadataSchema(NAME_RECORD_V2_SCHEMA, 1);
    Schema writerRmdSchema = RmdSchemaGenerator.generateMetadataSchema(writerValueSchema, 1);

    Schema inputFileSchema = new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
        .setValueSchema(NAME_RECORD_V2_SCHEMA)
        .setFieldSchema(DEFAULT_RMD_FIELD_PROP, inputRmdSchema)
        .build();

    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        inputFileSchema,
        "key",
        "value",
        DEFAULT_RMD_FIELD_PROP,
        NONE,
        null,
        writerValueSchema,
        writerRmdSchema);

    GenericRecord record = new GenericData.Record(inputFileSchema);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    valueRecord.put("age", 42);
    record.put("value", valueRecord);
    record.put(DEFAULT_RMD_FIELD_PROP, null);

    Assert.assertNull(recordReader.getRmdValue(new AvroWrapper<>(record), NullWritable.get()));
  }

  @Test
  public void testFromPropsWiresWriterSchemaAndProjects() {
    // Closes the loop with the MR/Spark plumbing: the writer value schema prop (set on the job conf) drives fromProps
    // to build a projecting reader.
    Schema writerValueSchema = NAME_RECORD_V1_SCHEMA;

    Properties props = new Properties();
    props.setProperty(SCHEMA_STRING_PROP, STRING_TO_NAME_RECORD_V2_SCHEMA.toString());
    props.setProperty(KEY_FIELD_PROP, "key");
    props.setProperty(VALUE_FIELD_PROP, "value");
    props.setProperty(WRITER_VALUE_SCHEMA_STRING_PROP, writerValueSchema.toString());

    VeniceAvroRecordReader recordReader = VeniceAvroRecordReader.fromProps(new VeniceProperties(props));

    GenericRecord record = new GenericData.Record(STRING_TO_NAME_RECORD_V2_SCHEMA);
    record.put("key", "123");
    GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
    valueRecord.put("firstName", "FN");
    valueRecord.put("lastName", "LN");
    valueRecord.put("age", 42);
    record.put("value", valueRecord);

    Object projectedValue = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());
    Assert.assertTrue(projectedValue instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) projectedValue).getSchema(), writerValueSchema);
    Assert.assertNull(((GenericRecord) projectedValue).getSchema().getField("age"));
    Assert.assertEquals(((GenericRecord) projectedValue).get("firstName").toString(), "FN");
  }

  @Test(dataProvider = "Boolean-and-EtlTransformations")
  public void testRecordReaderForETLInput(
      boolean nullDefaultValue,
      ETLValueSchemaTransformation etlValueSchemaTransformation) {
    Schema keySchema = STRING_SCHEMA;

    Schema valueSchema;
    switch (etlValueSchemaTransformation) {
      case NONE:
        valueSchema = Schema.createUnion(Arrays.asList(INT_SCHEMA, STRING_SCHEMA, Schema.create(Schema.Type.NULL)));
        break;
      case ADD_NULL_TO_UNION:
        valueSchema = Schema.createUnion(Arrays.asList(INT_SCHEMA, STRING_SCHEMA));
        break;
      case UNIONIZE_WITH_NULL:
        valueSchema = INT_SCHEMA;
        break;
      default:
        throw new IllegalArgumentException("Invalid ETL Value schema transformation: " + etlValueSchemaTransformation);
    }

    Schema fileSchema;
    if (nullDefaultValue) {
      fileSchema = TestWriteUtils.getETLFileSchemaWithNullDefaultValue(keySchema, valueSchema);
    } else {
      fileSchema = TestWriteUtils.getETLFileSchema(keySchema, valueSchema);
    }
    ETLValueSchemaTransformation inferredEtlTransformation = ETLValueSchemaTransformation.fromSchema(valueSchema);
    Assert.assertEquals(inferredEtlTransformation, etlValueSchemaTransformation);

    VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
        fileSchema,
        DEFAULT_KEY_FIELD_PROP,
        DEFAULT_VALUE_FIELD_PROP,
        DEFAULT_RMD_FIELD_PROP,
        etlValueSchemaTransformation,
        null);

    Schema dummyValueRecordSchema = Schema.createRecord(
        "valueWrapper",
        null,
        null,
        false,
        Collections.singletonList(
            AvroCompatibilityHelper.newField(null).setName(DEFAULT_VALUE_FIELD_PROP).setSchema(valueSchema).build()));
    for (int i = 0; i < 10; i++) {
      Object key = RANDOM_RECORD_GENERATOR.randomGeneric(keySchema);
      Object value;
      if (i == 0) {
        value = null;
      } else {
        // RandomRecordGenerator cannot handle union schemas. So, we need to create a dummy record to generate a value.
        value = ((GenericRecord) RANDOM_RECORD_GENERATOR.randomGeneric(dummyValueRecordSchema))
            .get(DEFAULT_VALUE_FIELD_PROP);
      }

      GenericRecord record = generateRandomEtlRecord(fileSchema, key, value);
      Object extractedValue = recordReader.getAvroValue(new AvroWrapper<>(record), NullWritable.get());

      Assert.assertEquals(value, extractedValue);
    }
  }

  private GenericRecord generateRandomEtlRecord(Schema fileSchema, Object key, Object value) {
    GenericRecord record = new GenericData.Record(fileSchema);
    record.put(DEFAULT_KEY_FIELD_PROP, key);
    record.put(DEFAULT_VALUE_FIELD_PROP, value);

    record.put("offset", (long) RandomGenUtils.getRandomIntWithin(Integer.MAX_VALUE));
    record.put("DELETED_TS", null);
    record.put("metadata", new HashMap<>());

    return record;
  }
}
