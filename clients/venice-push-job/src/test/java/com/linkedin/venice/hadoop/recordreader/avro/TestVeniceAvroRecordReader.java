package com.linkedin.venice.hadoop.recordreader.avro;

import static com.linkedin.venice.etl.ETLValueSchemaTransformation.ADD_NULL_TO_UNION;
import static com.linkedin.venice.etl.ETLValueSchemaTransformation.NONE;
import static com.linkedin.venice.etl.ETLValueSchemaTransformation.UNIONIZE_WITH_NULL;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.TestWriteUtils.INT_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    VeniceAvroRecordReader recordReader =
        new VeniceAvroRecordReader(STRING_TO_NAME_RECORD_V1_SCHEMA, "key", "value", NONE, updateSchema);

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
