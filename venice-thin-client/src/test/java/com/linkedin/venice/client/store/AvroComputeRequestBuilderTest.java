package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.compute.ComputeRequestWrapper.*;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.*;
import static org.mockito.Mockito.*;


public class AvroComputeRequestBuilderTest {
  private static final Schema VALID_RECORD_SCHEMA = Schema.parse("{\n" + "\t\"type\": \"record\",\n"
      + "\t\"name\": \"record_schema\",\n" + "\t\"fields\": [\n"
      + "\t\t{\"name\": \"int_field\", \"type\": \"int\", \"default\": 0, \"doc\": \"doc for int_field\"},\n"
      + "\t\t{\"name\": \"float_field\", \"type\": \"float\", \"doc\": \"doc for float_field\"},\n" + "\t\t{\n"
      + "\t\t\t\"name\": \"record_field\",\n" + "\t\t\t\"namespace\": \"com.linkedin.test\",\n" + "\t\t\t\"type\": {\n"
      + "\t\t\t\t\"name\": \"Record1\",\n" + "\t\t\t\t\"type\": \"record\",\n" + "\t\t\t\t\"fields\": [\n"
      + "\t\t\t\t\t{\"name\": \"nested_field1\", \"type\": \"double\", \"doc\": \"doc for nested field\"}\n"
      + "\t\t\t\t]\n" + "\t\t\t}\n" + "\t\t},\n"
      + "\t\t{\"name\": \"float_array_field1\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
      + "\t\t{\"name\": \"float_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
      + "\t\t{\"name\": \"int_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}\n" + "\t]\n" + "}");

  private static final Schema ARRAY_SCHEMA = Schema.parse("{\"type\": \"array\", \"items\": \"float\"}");

  private static final Set<String> keys = new HashSet<>();
  private static final List<Float> dotProductParam = Arrays.asList(1.0f, 2.0f);
  private static final List<Float> cosineSimilarityParam = Arrays.asList(3.0f, 4.0f);
  private static final List<Float> hadamardProductParam = Arrays.asList(5.5f, 6.6f);

  @Test
  public void testComputeRequestBuilder() {
    AbstractAvroStoreClient mockClient = getMockClient();
    doReturn("testStore").when(mockClient).getStoreName();
    ArgumentCaptor<ComputeRequestWrapper> computeRequestCaptor = ArgumentCaptor.forClass(ComputeRequestWrapper.class);
    ArgumentCaptor<Set> keysCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Schema> resultSchemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<Optional> statsCaptor = ArgumentCaptor.forClass(Optional.class);
    ArgumentCaptor<Long> preRequestTimeCaptor = ArgumentCaptor.forClass(Long.class);
    Time mockTime = Mockito.mock(Time.class);
    long preRequestTimeInNS = 1234;
    doReturn(preRequestTimeInNS).when(mockTime).nanoseconds();

    AvroComputeRequestBuilderV1<String> computeRequestBuilder = new AvroComputeRequestBuilderV1(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty(), mockTime);
    computeRequestBuilder.project("float_field", "record_field")
        .project("int_field")
        .dotProduct("float_array_field1", dotProductParam, "float_array_field1_dot_product_result")
        .dotProduct("float_array_field2", dotProductParam, "float_array_field2_dot_product_result")
        .dotProduct("float_array_field2", dotProductParam, "float_array_field2_another_dot_product_result")
        .cosineSimilarity("float_array_field1", cosineSimilarityParam, "float_array_field1_cosine_similarity_result")
        .cosineSimilarity("float_array_field2", cosineSimilarityParam, "float_array_field2_cosine_similarity_result")
        .cosineSimilarity("float_array_field2", cosineSimilarityParam, "float_array_field2_another_cosine_similarity_result")
        .execute(keys);

    verify(mockClient).compute(computeRequestCaptor.capture(), keysCaptor.capture(), resultSchemaCaptor.capture(),
        statsCaptor.capture(), preRequestTimeCaptor.capture());
    String expectedSchema = "{\"type\":\"record\",\"name\":\"testStore_VeniceComputeResult\",\"doc\":\"\",\"fields\":[{\"name\":\"float_field\",\"type\":\"float\",\"doc\":\"\"},{\"name\":\"record_field\",\"type\":{\"type\":\"record\",\"name\":\"Record1\",\"fields\":[{\"name\":\"nested_field1\",\"type\":\"double\",\"doc\":\"doc for nested field\"}]},\"doc\":\"\"},{\"name\":\"int_field\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field1_dot_product_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field2_dot_product_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field2_another_dot_product_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field1_cosine_similarity_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field2_cosine_similarity_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field2_another_cosine_similarity_result\",\"type\":\"double\",\"doc\":\"\",\"default\":0},{\"name\":\"__veniceComputationError__\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"\",\"default\":{}}]}";
    Assert.assertEquals(resultSchemaCaptor.getValue().toString(), expectedSchema);
    Assert.assertEquals(keysCaptor.getValue(), keys);
    Assert.assertFalse(statsCaptor.getValue().isPresent());
    Assert.assertEquals(preRequestTimeCaptor.getValue().longValue(), preRequestTimeInNS);
    ComputeRequestWrapper capturedComputeRequest = computeRequestCaptor.getValue();
    Assert.assertNotNull(capturedComputeRequest);
    Assert.assertEquals(capturedComputeRequest.getResultSchemaStr().toString(), expectedSchema);
    Assert.assertEquals(capturedComputeRequest.getOperations().size(), 6);
    Assert.assertEquals(capturedComputeRequest.getComputeRequestVersion(), COMPUTE_REQUEST_VERSION_V1);

    List<Float> expectedDotProductParam = new ArrayList<>();
    for (Float f : dotProductParam) {
      expectedDotProductParam.add(f);
    }
    ComputeOperation computeOperation1 = (ComputeOperation)capturedComputeRequest.getOperations().get(0);
    Assert.assertNotNull(computeOperation1);
    Assert.assertEquals(computeOperation1.operationType, DOT_PRODUCT.getValue());
    DotProduct dotProduct1 = (DotProduct)computeOperation1.operation;
    Assert.assertNotNull(dotProduct1);
    Assert.assertEquals(dotProduct1.field.toString(), "float_array_field1");
    Assert.assertEquals(dotProduct1.resultFieldName.toString(), "float_array_field1_dot_product_result");
    Assert.assertEquals(dotProduct1.dotProductParam, expectedDotProductParam);

    ComputeOperation computeOperation2 = (ComputeOperation)capturedComputeRequest.getOperations().get(1);
    Assert.assertNotNull(computeOperation2);
    Assert.assertEquals(computeOperation2.operationType, DOT_PRODUCT.getValue());
    DotProduct dotProduct2 = (DotProduct)computeOperation2.operation;
    Assert.assertNotNull(dotProduct2);
    Assert.assertEquals(dotProduct2.field.toString(), "float_array_field2");
    Assert.assertEquals(dotProduct2.resultFieldName.toString(), "float_array_field2_dot_product_result");
    Assert.assertEquals(dotProduct2.dotProductParam, expectedDotProductParam);

    ComputeOperation computeOperation3 = (ComputeOperation)capturedComputeRequest.getOperations().get(2);
    Assert.assertNotNull(computeOperation3);
    Assert.assertEquals(computeOperation3.operationType, DOT_PRODUCT.getValue());
    DotProduct dotProduct3 = (DotProduct)computeOperation3.operation;
    Assert.assertNotNull(dotProduct3);
    Assert.assertEquals(dotProduct3.field.toString(), "float_array_field2");
    Assert.assertEquals(dotProduct3.resultFieldName.toString(), "float_array_field2_another_dot_product_result");
    Assert.assertEquals(dotProduct3.dotProductParam, expectedDotProductParam);

    List<Float> expectedCosineSimilarityParam = new ArrayList<>();
    for (Float f : cosineSimilarityParam) {
      expectedCosineSimilarityParam.add(f);
    }
    ComputeOperation computeOperation4 = (ComputeOperation)capturedComputeRequest.getOperations().get(3);
    Assert.assertNotNull(computeOperation4);
    Assert.assertEquals(computeOperation4.operationType, COSINE_SIMILARITY.getValue());
    CosineSimilarity cosineSimilarity1 = (CosineSimilarity) computeOperation4.operation;
    Assert.assertNotNull(cosineSimilarity1);
    Assert.assertEquals(cosineSimilarity1.field.toString(), "float_array_field1");
    Assert.assertEquals(cosineSimilarity1.resultFieldName.toString(), "float_array_field1_cosine_similarity_result");
    Assert.assertEquals(cosineSimilarity1.cosSimilarityParam, expectedCosineSimilarityParam);

    ComputeOperation computeOperation5 = (ComputeOperation)capturedComputeRequest.getOperations().get(4);
    Assert.assertNotNull(computeOperation5);
    Assert.assertEquals(computeOperation5.operationType, COSINE_SIMILARITY.getValue());
    CosineSimilarity cosineSimilarity2 = (CosineSimilarity) computeOperation5.operation;
    Assert.assertNotNull(cosineSimilarity2);
    Assert.assertEquals(cosineSimilarity2.field.toString(), "float_array_field2");
    Assert.assertEquals(cosineSimilarity2.resultFieldName.toString(), "float_array_field2_cosine_similarity_result");
    Assert.assertEquals(cosineSimilarity2.cosSimilarityParam, expectedCosineSimilarityParam);

    ComputeOperation computeOperation6 = (ComputeOperation)capturedComputeRequest.getOperations().get(5);
    Assert.assertNotNull(computeOperation6);
    Assert.assertEquals(computeOperation6.operationType, COSINE_SIMILARITY.getValue());
    CosineSimilarity cosineSimilarity3 = (CosineSimilarity) computeOperation6.operation;
    Assert.assertNotNull(cosineSimilarity3);
    Assert.assertEquals(cosineSimilarity3.field.toString(), "float_array_field2");
    Assert.assertEquals(cosineSimilarity3.resultFieldName.toString(), "float_array_field2_another_cosine_similarity_result");
    Assert.assertEquals(cosineSimilarity3.cosSimilarityParam, expectedCosineSimilarityParam);

    /**
     * Test AvroComputeRequestBuilderV2 for compute request version 2.
     */
    AbstractAvroStoreClient mockClient2 = getMockClient();
    doReturn("testStore").when(mockClient2).getStoreName();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder2 = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient2, Optional.empty(), Optional.empty(), mockTime, false, null, null);

    computeRequestBuilder2
        .hadamardProduct("float_array_field1", hadamardProductParam, "float_array_field1_hadamard_product_result")
        .project("int_field")
        .dotProduct("float_array_field1", dotProductParam, "float_array_field1_dot_product_result")
        .cosineSimilarity("float_array_field2", cosineSimilarityParam, "float_array_field2_cosine_similarity_result")
        .execute(keys);

    verify(mockClient2).compute(computeRequestCaptor.capture(), keysCaptor.capture(), resultSchemaCaptor.capture(),
        statsCaptor.capture(), preRequestTimeCaptor.capture());

    expectedSchema = "{\"type\":\"record\",\"name\":\"testStore_VeniceComputeResult\",\"doc\":\"\",\"fields\":[{\"name\":\"int_field\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"float_array_field1_dot_product_result\",\"type\":[\"null\",\"float\"],\"doc\":\"\",\"default\":null},{\"name\":\"float_array_field2_cosine_similarity_result\",\"type\":[\"null\",\"float\"],\"doc\":\"\",\"default\":null},{\"name\":\"float_array_field1_hadamard_product_result\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"float\"}],\"doc\":\"\",\"default\":null},{\"name\":\"__veniceComputationError__\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"\",\"default\":{}}]}";
    Assert.assertEquals(resultSchemaCaptor.getValue().toString(), expectedSchema);
    Assert.assertEquals(keysCaptor.getValue(), keys);
    Assert.assertFalse(statsCaptor.getValue().isPresent());
    Assert.assertEquals(preRequestTimeCaptor.getValue().longValue(), preRequestTimeInNS);
    capturedComputeRequest = computeRequestCaptor.getValue();
    Assert.assertNotNull(capturedComputeRequest);
    Assert.assertEquals(capturedComputeRequest.getResultSchemaStr().toString(), expectedSchema);
    Assert.assertEquals(capturedComputeRequest.getOperations().size(), 3);
    /**
     * Compute request version should be {@link LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST}
     * if {@link AvroComputeRequestBuilderV2#hadamardProduct(String, List, String)} is invoked.
     */
    Assert.assertEquals(capturedComputeRequest.getComputeRequestVersion(), LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);

    // Verify hadamard-product parameter
    List<Float> expectedHadamardProductParam = new ArrayList<>();
    for (Float f : hadamardProductParam) {
      expectedHadamardProductParam.add(f);
    }
    ComputeOperation hadamardProductOperation = (ComputeOperation)capturedComputeRequest.getOperations().get(2);
    Assert.assertNotNull(hadamardProductOperation);
    Assert.assertEquals(hadamardProductOperation.operationType, HADAMARD_PRODUCT.getValue());
    HadamardProduct hadamardProduct = (HadamardProduct)hadamardProductOperation.operation;
    Assert.assertNotNull(hadamardProduct);
    Assert.assertEquals(hadamardProduct.field.toString(), "float_array_field1");
    Assert.assertEquals(hadamardProduct.resultFieldName.toString(), "float_array_field1_hadamard_product_result");
    Assert.assertEquals(hadamardProduct.hadamardProductParam, expectedHadamardProductParam);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Only value schema with 'RECORD' type is supported")
  public void testComputeAgainstNonRecordSchema() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(ARRAY_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Unknown project field.*")
  public void testProjectUnknownField() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.project("some_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Unknown DOT_PRODUCT field.*")
  public void testDotProductAgainstUnknownField() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("some_unknown_field", dotProductParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Unknown COSINE_SIMILARITY field.*")
  public void testCosineSimilarityAgainstUnknownField() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("some_unknown_field", cosineSimilarityParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*isn't with 'ARRAY' type")
  public void testDotProductAgainstNonFloatArrayField1() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("int_field", dotProductParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*isn't with 'ARRAY' type")
  public void testCosineSimilarityAgainstNonFloatArrayField1() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("int_field", cosineSimilarityParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*int_array_field2 isn't an 'ARRAY' of 'FLOAT'")
  public void testDotProductAgainstNonFloatArrayField2() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("int_array_field2", dotProductParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*int_array_field2 isn't an 'ARRAY' of 'FLOAT'")
  public void testCosineSimilarityAgainstNonFloatArrayField2() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("int_array_field2", cosineSimilarityParam, "new_unknown_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*__veniceComputationError__ is reserved.*")
  public void testInvalidSchemaWithReservedFieldName() {
    String invalidSchemaStr = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"invalid_value_schema\",\n"
        + "\t\"fields\": [\n" + "\t\t{\"name\": \"int_field\", \"type\": \"int\", \"default\": 0},\n"
        + "\t\t{\"name\": \""+ VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME + "\", \"type\": \"string\"}\n" + "\t]\n" + "}";
    AbstractAvroStoreClient mockClient = getMockClient();
     new AvroComputeRequestBuilderV2(Schema.parse(invalidSchemaStr),
        mockClient, Optional.empty(), Optional.empty());
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".* __veniceComputationError__ is reserved.*")
  public void testDotProductWhileResultFieldUsingReservedFieldName() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("float_array_field1", dotProductParam, VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME);
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".* __veniceComputationError__ is reserved.*")
  public void testCosineSimilarityWhileResultFieldUsingReservedFieldName() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("float_array_field1", cosineSimilarityParam, VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME);
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "DOT_PRODUCT result field: int_field collides with the fields defined in value schema")
  public void testDotProductWhileResultFieldUsingExistingFieldName() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("float_array_field1", dotProductParam, "int_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "COSINE_SIMILARITY result field: int_field collides with the fields defined in value schema")
  public void testCosineSimilarityWhileResultFieldUsingExistingFieldName() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("float_array_field1", cosineSimilarityParam, "int_field");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "DOT_PRODUCT result field: same_field_name has been specified more than once")
  public void testDotProductWhileResultFieldUsingSameFieldNameMultipleTimes() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("float_array_field1", dotProductParam, "same_field_name");
    computeRequestBuilder.dotProduct("float_array_field2", dotProductParam, "same_field_name");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "COSINE_SIMILARITY result field: same_field_name has been specified more than once")
  public void testCosineSimilarityWhileResultFieldUsingSameFieldNameMultipleTimes() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.cosineSimilarity("float_array_field1", cosineSimilarityParam, "same_field_name");
    computeRequestBuilder.cosineSimilarity("float_array_field2", cosineSimilarityParam, "same_field_name");
    computeRequestBuilder.execute(keys);
  }

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "COSINE_SIMILARITY result field: same_field_name has been specified more than once")
  public void testDifferentOperationsWhileResultFieldUsingSameFieldNameMultipleTimes() {
    AbstractAvroStoreClient mockClient = getMockClient();
    AvroComputeRequestBuilderV2<String> computeRequestBuilder = new AvroComputeRequestBuilderV2(VALID_RECORD_SCHEMA,
        mockClient, Optional.empty(), Optional.empty());
    computeRequestBuilder.dotProduct("float_array_field1", dotProductParam, "same_field_name");
    computeRequestBuilder.cosineSimilarity("float_array_field2", cosineSimilarityParam, "same_field_name");
    computeRequestBuilder.execute(keys);
  }

  private AbstractAvroStoreClient getMockClient() {
    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    String storeName = TestUtils.getUniqueString("store_for_mock_client");
    doReturn(storeName).when(mockClient).getStoreName();
    return mockClient;
  }
}