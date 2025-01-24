package com.linkedin.venice.compute;

import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.primitive.PrimitiveFloatArrayList;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.Count;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComputeUtilsTest {
  @Test
  public void testComputeResult() throws IOException {
    // Input/output schemas
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("testMergeSchema.avsc"));
    List<Schema.Field> resultSchemaFields = new ArrayList<>();
    resultSchemaFields.add(
        AvroCompatibilityHelper.createSchemaField(
            "IntMapField",
            Schema.createMap(Schema.create(Schema.Type.INT)),
            "doc",
            new HashMap<>()));
    resultSchemaFields.add(
        AvroCompatibilityHelper.createSchemaField("StringListFieldCount", Schema.create(Schema.Type.INT), "doc", -1));
    resultSchemaFields.add(
        AvroCompatibilityHelper.createSchemaField(
            VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME,
            Schema.createMap(Schema.create(Schema.Type.STRING)),
            "doc",
            new HashMap<>()));
    Schema resultSchema = Schema.createRecord("Result", "doc", "com.acme", false, resultSchemaFields);

    // Other boilerplate
    int computeVersion = 4;
    List<ComputeOperation> operations = new ArrayList<>();
    ComputeOperation op1 = new ComputeOperation();
    op1.setOperationType(ComputeOperationType.COUNT.getValue());
    Count count = new Count();
    count.setField("StringListField");
    count.setResultFieldName("StringListFieldCount");
    op1.setOperation(count);
    operations.add(op1);
    List<Schema.Field> operationResultFields = ComputeUtils.getOperationResultFields(operations, resultSchema);
    Map<String, Object> sharedContext = new HashMap<>();
    GenericRecord inputRecord = new GenericData.Record(valueSchema);
    inputRecord.put("StringListField", new ArrayList<>());
    GenericRecord outputRecord = new GenericData.Record(resultSchema);

    // Code under test
    ComputeUtils.computeResult(operations, operationResultFields, sharedContext, inputRecord, outputRecord);

    assertNull(outputRecord.get("IntMapField"));
    assertEquals(outputRecord.get("StringListFieldCount"), 0);
    assertTrue(
        ((Map<String, String>) outputRecord.get(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).isEmpty());
  }

  @Test
  public void testGetNullableFieldValueAsList_NonNullValue() {
    GenericRecord record = createGetNullableFieldValueAsListRecord();
    List<Integer> expectedList = Arrays.asList(1, 2, 3);
    record.put("listField", expectedList);
    Schema.Field field = record.getSchema().getField("listField");
    List<Integer> resultList = ComputeUtils.getNullableFieldValueAsList(record, field);
    Assert.assertEquals(resultList, expectedList);
  }

  @Test
  public void testGetNullableFieldValueAsList_NullValue() {
    GenericRecord record = createGetNullableFieldValueAsListRecord();
    record.put("listField", null);
    Schema.Field field = record.getSchema().getField("listField");
    List<Integer> resultList = ComputeUtils.getNullableFieldValueAsList(record, field);
    Assert.assertEquals(resultList, Collections.emptyList());
  }

  @Test
  public void testGetNullableFieldValueAsList_FieldNotList() {
    GenericRecord record = createGetNullableFieldValueAsListRecord();
    record.put("nonListField", 123);
    Schema.Field field = record.getSchema().getField("nonListField");
    assertThrows(IllegalArgumentException.class, () -> ComputeUtils.getNullableFieldValueAsList(record, field));
  }

  @Test
  public void testRemoveAvroIllegalCharacter() {
    String input = "This*is$an@illegal^Avro#name!";
    String expectedOutput = "This_is_an_illegal_Avro_name_";
    String result = ComputeUtils.removeAvroIllegalCharacter(input);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testHadamardProduct() {
    List<Float> list1 = Arrays.asList(1.0f, 2.0f, 3.0f);
    List<Float> list2 = Arrays.asList(4.0f, 5.0f, 6.0f);
    List<Float> expectedOutput = Arrays.asList(4.0f, 10.0f, 18.0f);
    List<Float> result = ComputeUtils.hadamardProduct(list1, list2);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testHadamardProduct_PrimitiveFloatList() {
    PrimitiveFloatList list1 = createPrimitiveFloatList(1.0f, 2.0f, 3.0f);
    PrimitiveFloatList list2 = createPrimitiveFloatList(4.0f, 5.0f, 6.0f);
    List<Float> expectedOutput = Arrays.asList(4.0f, 10.0f, 18.0f);
    List<Float> result = ComputeUtils.hadamardProduct(list1, list2);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testSquaredL2Norm() {
    List<Float> list = Arrays.asList(1.0f, 2.0f, 3.0f);
    float expectedOutput = 14.0f;
    float result = ComputeUtils.squaredL2Norm(list);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testSquaredL2Norm_PrimitiveFloatList() {
    PrimitiveFloatList list = createPrimitiveFloatList(1.0f, 2.0f, 3.0f);
    float expectedOutput = 14.0f;
    float result = ComputeUtils.squaredL2Norm(list);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testDotProduct() {
    List<Float> list1 = Arrays.asList(1.0f, 2.0f, 3.0f);
    List<Float> list2 = Arrays.asList(4.0f, 5.0f, 6.0f);
    float expectedOutput = 32.0f;
    float result = ComputeUtils.dotProduct(list1, list2);
    Assert.assertEquals(result, expectedOutput);
  }

  @Test
  public void testDotProduct_PrimitiveFloatList() {
    PrimitiveFloatList list1 = createPrimitiveFloatList(1.0f, 2.0f, 3.0f);
    PrimitiveFloatList list2 = createPrimitiveFloatList(4.0f, 5.0f, 6.0f);
    float expectedOutput = 32.0f;
    float result = ComputeUtils.dotProduct(list1, list2);
    Assert.assertEquals(result, expectedOutput);
  }

  private static GenericRecord createGetNullableFieldValueAsListRecord() {
    Schema schema = SchemaBuilder.record("SampleSchema")
        .fields()
        .name("listField")
        .type()
        .array()
        .items()
        .intType()
        .noDefault()
        .requiredInt("nonListField")
        .endRecord();
    return new GenericData.Record(schema);
  }

  private static PrimitiveFloatList createPrimitiveFloatList(float... values) {
    PrimitiveFloatList list = new PrimitiveFloatArrayList(values.length);
    for (float value: values) {
      list.add(value);
    }
    return list;
  }

  @Test
  public void testValidateNullableFieldAndGetErrorMsg_NullField_AllowedNullValue() {
    TestReadComputeOperator operator = new TestReadComputeOperator(true, "field", "result");

    Schema schema =
        SchemaBuilder.record("SampleSchema").fields().name("field").type().nullable().intType().noDefault().endRecord();
    GenericRecord valueRecord = new GenericData.Record(schema);

    Schema.Field field = valueRecord.getSchema().getField("field");
    String errorMsg = ComputeUtils.validateNullableFieldAndGetErrorMsg(operator, valueRecord, field, "field");
    Assert.assertNull(errorMsg);
  }

  @Test
  public void testValidateNullableFieldAndGetErrorMsg_NullField_NotAllowedNullValue() {
    TestReadComputeOperator operator = new TestReadComputeOperator(false, "field", "result");

    Schema schema =
        SchemaBuilder.record("SampleSchema").fields().name("field").type().nullable().intType().noDefault().endRecord();
    GenericRecord valueRecord = new GenericData.Record(schema);

    Schema.Field field = valueRecord.getSchema().getField("field");
    String errorMsg = ComputeUtils.validateNullableFieldAndGetErrorMsg(operator, valueRecord, field, "field");
    Assert.assertNotNull(errorMsg);
    Assert.assertEquals(
        errorMsg,
        "Failed to execute compute request as the field field is not allowed to be null for " + operator
            + " in value record.");
  }

  @Test
  public void testValidateNullableFieldAndGetErrorMsg_FieldNotNull_NotAllowedNullValue() {
    TestReadComputeOperator operator = new TestReadComputeOperator(false, "field", "result");

    Schema schema = SchemaBuilder.record("SampleSchema").fields().requiredInt("field").endRecord();
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("field", 123);

    Schema.Field field = valueRecord.getSchema().getField("field");
    String errorMsg = ComputeUtils.validateNullableFieldAndGetErrorMsg(operator, valueRecord, field, "field");
    Assert.assertNull(errorMsg);
  }

  @Test
  public void testValidateNullableFieldAndGetErrorMsg_FieldNotNull_AllowedNullValue() {
    TestReadComputeOperator operator = new TestReadComputeOperator(true, "field", "result");

    Schema schema = SchemaBuilder.record("SampleSchema").fields().requiredInt("field").endRecord();
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("field", 123);

    Schema.Field field = valueRecord.getSchema().getField("field");
    String errorMsg = ComputeUtils.validateNullableFieldAndGetErrorMsg(operator, valueRecord, field, "field");
    Assert.assertNull(errorMsg);
  }

  private static class TestReadComputeOperator implements ReadComputeOperator {
    private final boolean allowFieldToBeNull;
    private final String operatorFieldName;
    private final String resultFieldName;

    public TestReadComputeOperator(boolean allowFieldToBeNull, String operatorFieldName, String resultFieldName) {
      this.allowFieldToBeNull = allowFieldToBeNull;
      this.operatorFieldName = operatorFieldName;
      this.resultFieldName = resultFieldName;
    }

    @Override
    public String getOperatorFieldName(ComputeOperation operation) {
      return operatorFieldName;
    }

    @Override
    public String getResultFieldName(ComputeOperation operation) {
      return resultFieldName;
    }

    @Override
    public void compute(
        ComputeOperation operation,
        Schema.Field operatorInputField,
        Schema.Field resultField,
        GenericRecord inputValueRecord,
        GenericRecord outputRecord,
        Map<String, String> errorMap,
        Map<String, Object> sharedContext) {
    }

    @Override
    public boolean allowFieldValueToBeNull() {
      return allowFieldToBeNull;
    }

    @Override
    public void putDefaultResult(GenericRecord outputRecord, Schema.Field field) {
    }

    @Override
    public String toString() {
      return this.getClass().toString();
    }
  }
}
