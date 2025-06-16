package com.linkedin.venice.client.store;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.schema.SchemaReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for {@link AvroComputeAggregationRequestBuilder} which verifies the request building
 * and execution with various configurations and edge cases.
 */
public class AvroComputeAggregationRequestBuilderTest {
  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final int TOP_K = 10;
  private static final int MAX_CONCURRENT_REQUESTS = 100;
  private static final String STORE_NAME = "test_store";
  private static final String KEY_PREFIX = "key_";
  private static final String STRING_FIELD_NAME = "string_field";
  private static final String INT_FIELD_NAME = "int_field";
  private static final String LONG_FIELD_NAME = "long_field";
  private static final String FLOAT_FIELD_NAME = "float_field";
  private static final String DOUBLE_FIELD_NAME = "double_field";
  private static final String BOOLEAN_FIELD_NAME = "boolean_field";
  private static final String UNSUPPORTED_FIELD_NAME = "unsupported_field";

  @Mock
  private AvroGenericReadComputeStoreClient<String, GenericRecord> storeClient;
  @Mock
  private SchemaReader schemaReader;
  @Mock
  private Schema valueSchema;
  @Mock
  private Schema.Field field1;
  @Mock
  private Schema.Field field2;
  @Mock
  private Schema field1Schema;
  @Mock
  private Schema field2Schema;
  @Mock
  private Schema arrayItemSchema;

  private AvroComputeRequestBuilderV3<String> computeRequestBuilder;
  private AvroComputeAggregationRequestBuilder<String> builder;
  private List<String> keys;
  private Map<String, Integer> fieldTopKMap;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    setupSchemaMocks();
    setupTestData();

    // Setup store name first
    when(storeClient.getStoreName()).thenReturn(STORE_NAME);

    // Create computeRequestBuilder with proper mocks
    computeRequestBuilder = spy(new AvroComputeRequestBuilderV3<>(storeClient, schemaReader));

    // Setup compute() after creating spy
    when(storeClient.compute()).thenReturn(computeRequestBuilder);

    // Create the builder
    builder = new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);
  }

  private void setupSchemaMocks() {
    when(schemaReader.getLatestValueSchemaId()).thenReturn(1);
    when(schemaReader.getValueSchema(1)).thenReturn(valueSchema);
    when(valueSchema.getType()).thenReturn(Schema.Type.RECORD);

    // Common elementType mocks
    Schema stringElementSchema = mock(Schema.class);
    when(stringElementSchema.getType()).thenReturn(Schema.Type.STRING);
    Schema intElementSchema = mock(Schema.class);
    when(intElementSchema.getType()).thenReturn(Schema.Type.INT);
    Schema longElementSchema = mock(Schema.class);
    when(longElementSchema.getType()).thenReturn(Schema.Type.LONG);
    Schema floatElementSchema = mock(Schema.class);
    when(floatElementSchema.getType()).thenReturn(Schema.Type.FLOAT);
    Schema doubleElementSchema = mock(Schema.class);
    when(doubleElementSchema.getType()).thenReturn(Schema.Type.DOUBLE);
    Schema booleanElementSchema = mock(Schema.class);
    when(booleanElementSchema.getType()).thenReturn(Schema.Type.BOOLEAN);
    Schema recordElementSchema = mock(Schema.class);
    when(recordElementSchema.getType()).thenReturn(Schema.Type.RECORD);

    // field1, field2: ARRAY<STRING>
    when(valueSchema.getField(FIELD_1)).thenReturn(field1);
    when(field1.schema()).thenReturn(field1Schema);
    when(field1Schema.getType()).thenReturn(Schema.Type.ARRAY);
    when(field1Schema.getElementType()).thenReturn(stringElementSchema);

    when(valueSchema.getField(FIELD_2)).thenReturn(field2);
    when(field2.schema()).thenReturn(field2Schema);
    when(field2Schema.getType()).thenReturn(Schema.Type.ARRAY);
    when(field2Schema.getElementType()).thenReturn(stringElementSchema);

    // string_field: ARRAY<STRING>
    Schema.Field stringField = mock(Schema.Field.class);
    Schema stringFieldSchema = mock(Schema.class);
    when(stringFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(stringFieldSchema.getElementType()).thenReturn(stringElementSchema);
    when(stringField.schema()).thenReturn(stringFieldSchema);
    when(valueSchema.getField(STRING_FIELD_NAME)).thenReturn(stringField);

    // int_field: ARRAY<INT>
    Schema.Field intField = mock(Schema.Field.class);
    Schema intFieldSchema = mock(Schema.class);
    when(intFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(intFieldSchema.getElementType()).thenReturn(intElementSchema);
    when(intField.schema()).thenReturn(intFieldSchema);
    when(valueSchema.getField(INT_FIELD_NAME)).thenReturn(intField);

    // long_field: ARRAY<LONG>
    Schema.Field longField = mock(Schema.Field.class);
    Schema longFieldSchema = mock(Schema.class);
    when(longFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(longFieldSchema.getElementType()).thenReturn(longElementSchema);
    when(longField.schema()).thenReturn(longFieldSchema);
    when(valueSchema.getField(LONG_FIELD_NAME)).thenReturn(longField);

    // float_field: ARRAY<FLOAT>
    Schema.Field floatField = mock(Schema.Field.class);
    Schema floatFieldSchema = mock(Schema.class);
    when(floatFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(floatFieldSchema.getElementType()).thenReturn(floatElementSchema);
    when(floatField.schema()).thenReturn(floatFieldSchema);
    when(valueSchema.getField(FLOAT_FIELD_NAME)).thenReturn(floatField);

    // double_field: ARRAY<DOUBLE>
    Schema.Field doubleField = mock(Schema.Field.class);
    Schema doubleFieldSchema = mock(Schema.class);
    when(doubleFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(doubleFieldSchema.getElementType()).thenReturn(doubleElementSchema);
    when(doubleField.schema()).thenReturn(doubleFieldSchema);
    when(valueSchema.getField(DOUBLE_FIELD_NAME)).thenReturn(doubleField);

    // boolean_field: ARRAY<BOOLEAN>
    Schema.Field booleanField = mock(Schema.Field.class);
    Schema booleanFieldSchema = mock(Schema.class);
    when(booleanFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(booleanFieldSchema.getElementType()).thenReturn(booleanElementSchema);
    when(booleanField.schema()).thenReturn(booleanFieldSchema);
    when(valueSchema.getField(BOOLEAN_FIELD_NAME)).thenReturn(booleanField);

    // unsupported_field: ARRAY<RECORD>
    Schema.Field unsupportedField = mock(Schema.Field.class);
    Schema unsupportedFieldSchema = mock(Schema.class);
    when(unsupportedFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(unsupportedFieldSchema.getElementType()).thenReturn(recordElementSchema);
    when(unsupportedField.schema()).thenReturn(unsupportedFieldSchema);
    when(valueSchema.getField(UNSUPPORTED_FIELD_NAME)).thenReturn(unsupportedField);
  }

  private void setupTestData() {
    keys = Arrays.asList("key1", "key2", "key3");
    fieldTopKMap = new HashMap<>();
    fieldTopKMap.put(FIELD_1, TOP_K);
    fieldTopKMap.put(FIELD_2, TOP_K);
  }

  @Test(description = "Should build request with valid parameters")
  public void testCountGroupByValue_ValidParameters() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    verify(computeRequestBuilder).project(eq(FIELD_1));
  }

  @Test(description = "Should handle multiple fields")
  public void testCountGroupByValue_ManyFields() {
    builder.countGroupByValue(TOP_K, FIELD_1, FIELD_2);

    verify(computeRequestBuilder).project(eq(FIELD_1));
    verify(computeRequestBuilder).project(eq(FIELD_2));
  }

  @DataProvider(name = "invalidTopK")
  public Object[][] getInvalidTopK() {
    return new Object[][] { { 0, "TopK must be positive" }, { -1, "TopK must be positive" },
        { Integer.MIN_VALUE, "TopK must be positive" } };
  }

  @Test(dataProvider = "invalidTopK", description = "Should reject invalid topK values")
  public void testCountGroupByValue_InvalidTopK(int topK, String expectedMessage) {
    VeniceClientException exception =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(topK, FIELD_1));
    assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @DataProvider(name = "invalidFieldNames")
  public Object[][] getInvalidFieldNames() {
    return new Object[][] { { null, "Field name cannot be null" }, { "", "Field name cannot be empty" },
        { "nonexistent", "Field not found in schema" } };
  }

  @Test(dataProvider = "invalidFieldNames", description = "Should reject invalid field names")
  public void testCountGroupByValue_InvalidFieldNames(String fieldName, String expectedMessage) {
    VeniceClientException exception =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, fieldName));
    assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @Test(description = "Should handle empty field name")
  public void testCountGroupByValue_EmptyFieldName() {
    assertThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, ""));
  }

  @Test(description = "Should build request properly with valid parameters")
  public void testExecute_ValidParameters() {
    builder.countGroupByValue(TOP_K, FIELD_1);

    // Verify the project method was called during countGroupByValue
    verify(computeRequestBuilder).project(eq(FIELD_1));
  }

  @Test(description = "Should handle null keys")
  public void testExecute_NullKeys() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    assertThrows(VeniceClientException.class, () -> builder.execute(null));
  }

  @Test(description = "Should handle empty keys")
  public void testExecute_EmptyKeys() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    assertThrows(VeniceClientException.class, () -> builder.execute(new HashSet<>()));
  }

  @Test(description = "Should handle concurrent requests")
  public void testCountGroupByValue_Concurrent() throws Exception {
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(() -> {
        try {
          builder.countGroupByValue(TOP_K, FIELD_1);
          latch.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    boolean completed = latch.await(5, TimeUnit.SECONDS);
    assertTrue(completed, "Timeout waiting for latch in concurrent test");
    for (Future<?> future: futures) {
      future.get();
    }

    verify(computeRequestBuilder, times(numThreads)).project(eq(FIELD_1));
    executor.shutdown();
  }

  @Test
  public void testCountGroupByValueWithValidFields() {
    builder.countGroupByValue(TOP_K, FIELD_1);

    // Verify the project method was called
    verify(computeRequestBuilder).project(eq(FIELD_1));
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithUnsupportedField() {
    builder.countGroupByValue(5, UNSUPPORTED_FIELD_NAME);
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithNullFieldName() {
    builder.countGroupByValue(5, (String) null);
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithEmptyFieldName() {
    builder.countGroupByValue(5, "");
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithInvalidTopK() {
    builder.countGroupByValue(0, STRING_FIELD_NAME);
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithNullFieldNames() {
    builder.countGroupByValue(5, (String[]) null);
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testCountGroupByValueWithEmptyFieldNames() {
    builder.countGroupByValue(5, new String[0]);
  }

  @DataProvider(name = "fieldTypes")
  public Object[][] getFieldTypes() {
    return new Object[][] { { STRING_FIELD_NAME, Schema.Type.STRING, true }, { INT_FIELD_NAME, Schema.Type.INT, true },
        { LONG_FIELD_NAME, Schema.Type.LONG, true }, { FLOAT_FIELD_NAME, Schema.Type.FLOAT, true },
        { DOUBLE_FIELD_NAME, Schema.Type.DOUBLE, true }, { BOOLEAN_FIELD_NAME, Schema.Type.BOOLEAN, true },
        { UNSUPPORTED_FIELD_NAME, Schema.Type.RECORD, false } };
  }

  @Test(dataProvider = "fieldTypes")
  public void testCountGroupByValue_FieldTypes(String fieldName, Schema.Type elementType, boolean shouldSucceed) {
    // Set field type
    Schema.Field field = mock(Schema.Field.class);
    Schema fieldSchema = mock(Schema.class);
    Schema elementSchema = mock(Schema.class);

    when(valueSchema.getField(fieldName)).thenReturn(field);
    when(field.schema()).thenReturn(fieldSchema);
    when(fieldSchema.getType()).thenReturn(shouldSucceed ? Schema.Type.ARRAY : Schema.Type.RECORD);
    if (shouldSucceed) {
      when(fieldSchema.getElementType()).thenReturn(elementSchema);
      when(elementSchema.getType()).thenReturn(elementType);
    }

    if (shouldSucceed) {
      // For supported types, should succeed
      builder.countGroupByValue(TOP_K, fieldName);
      verify(computeRequestBuilder).project(eq(fieldName));
    } else {
      // For unsupported types, should throw exception
      VeniceClientException exception =
          expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, fieldName));
      assertTrue(exception.getMessage().contains("not supported for count by value operation"));
    }
  }

  @Test(description = "Should validate field types")
  public void testCountGroupByValue_FieldTypeValidation() {
    // Test with non-array field
    Schema.Field nonArrayField = mock(Schema.Field.class);
    Schema nonArrayFieldSchema = mock(Schema.class);
    when(nonArrayFieldSchema.getType()).thenReturn(Schema.Type.STRING);
    when(nonArrayField.schema()).thenReturn(nonArrayFieldSchema);
    when(valueSchema.getField("nonArrayField")).thenReturn(nonArrayField);

    VeniceClientException exception =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, "nonArrayField"));
    assertTrue(exception.getMessage().contains("not supported for count by value operation"));

    // Test with array field
    Schema.Field arrayField = mock(Schema.Field.class);
    Schema arrayFieldSchema = mock(Schema.class);
    Schema elementSchema = mock(Schema.class);
    when(arrayFieldSchema.getType()).thenReturn(Schema.Type.ARRAY);
    when(arrayFieldSchema.getElementType()).thenReturn(elementSchema);
    when(elementSchema.getType()).thenReturn(Schema.Type.STRING);
    when(arrayField.schema()).thenReturn(arrayFieldSchema);
    when(valueSchema.getField("arrayField")).thenReturn(arrayField);

    builder.countGroupByValue(TOP_K, "arrayField");
    verify(computeRequestBuilder).project(eq("arrayField"));

    // Test with map field
    Schema.Field mapField = mock(Schema.Field.class);
    Schema mapFieldSchema = mock(Schema.class);
    Schema valueSchema = mock(Schema.class);
    when(mapFieldSchema.getType()).thenReturn(Schema.Type.MAP);
    when(mapFieldSchema.getValueType()).thenReturn(valueSchema);
    when(valueSchema.getType()).thenReturn(Schema.Type.STRING);
    when(mapField.schema()).thenReturn(mapFieldSchema);
    when(this.valueSchema.getField("mapField")).thenReturn(mapField);

    builder.countGroupByValue(TOP_K, "mapField");
    verify(computeRequestBuilder).project(eq("mapField"));
  }
}
