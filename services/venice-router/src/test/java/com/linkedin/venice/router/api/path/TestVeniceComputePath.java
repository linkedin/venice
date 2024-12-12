package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV2;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceComputePath {
  private static String resultSchemaStr = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },       "
      + "         { \"name\": \"member_score\", \"type\": \"double\" }        " + "  ]       " + " }       ";

  public static ComputeRequestV1 getComputeRequest() {
    DotProduct dotProduct = new DotProduct();
    dotProduct.field = "member_feature";
    List<Float> featureVector = new ArrayList<>(3);
    featureVector.add(Float.valueOf((float) 0.4));
    featureVector.add(Float.valueOf((float) 66.6));
    featureVector.add(Float.valueOf((float) 5.20));
    dotProduct.dotProductParam = featureVector;
    dotProduct.resultFieldName = "member_score";

    ComputeOperation operation = new ComputeOperation();
    operation.operationType = 0; // DotProduct
    operation.operation = dotProduct;

    List<Object> operationList = new ArrayList<>();
    operationList.add(operation);

    ComputeRequestV1 record = new ComputeRequestV1();
    record.operations = operationList;
    record.resultSchemaStr = resultSchemaStr;
    return record;
  }

  private static BasicFullHttpRequest getComputeHttpRequest(String resourceName, byte[] content, int apiVersion) {
    String uri = "/compute/" + resourceName;
    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, Unpooled.wrappedBuffer(content), 0, 0);
    request.headers().add(HttpConstants.VENICE_API_VERSION, apiVersion);
    return request;
  }

  public static BasicFullHttpRequest getComputeHttpRequest(
      String resourceName,
      ComputeRequestV1 request,
      List<ByteBuffer> keys,
      int apiVersion) {
    RecordSerializer<ComputeRequestV1> computeRequestSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRequestV1.getClassSchema());
    byte[] serializedComputeRequest = computeRequestSerializer.serialize(request);

    RecordSerializer<ByteBuffer> keySerializer = SerializerDeserializerFactory
        .getAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());
    byte[] serializedKeys = keySerializer.serializeObjects(keys);

    return getComputeHttpRequest(resourceName, ArrayUtils.addAll(serializedComputeRequest, serializedKeys), apiVersion);
  }

  private VenicePartitionFinder getVenicePartitionFinder(int partitionId) {
    VenicePartitionFinder mockedPartitionFinder = mock(VenicePartitionFinder.class);
    VenicePartitioner venicePartitioner = mock(VenicePartitioner.class);
    when(venicePartitioner.getPartitionId(any(ByteBuffer.class), anyInt())).thenReturn(partitionId);
    when(mockedPartitionFinder.findPartitioner(anyString(), anyInt())).thenReturn(venicePartitioner);
    return mockedPartitionFinder;
  }

  @Test
  public void testDeserializationCorrectness() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int versionNumber = 1;
    String resourceName = storeName + "_v" + versionNumber;

    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    ComputeRequestV1 computeRequest = getComputeRequest();
    RecordSerializer<ComputeRequestV1> computeRequestSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRequestV1.getClassSchema());
    int expectedLength = computeRequestSerializer.serialize(computeRequest).length;

    // test all compute request versions
    for (int apiVersion = 1; apiVersion <= LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST; apiVersion++) {
      BasicFullHttpRequest request = getComputeHttpRequest(resourceName, computeRequest, keys, apiVersion);

      VeniceComputePath computePath = new VeniceComputePath(
          storeName,
          versionNumber,
          resourceName,
          request,
          getVenicePartitionFinder(-1),
          10,
          false,
          -1,
          1,
          mock(RetryManager.class));
      Assert.assertEquals(computePath.getComputeRequestLengthInBytes(), expectedLength);
    }
  }

  @Test
  public void testComputeRequestVersionBackwardCompatible() {
    // generate a version 1 record
    ComputeRequestV1 computeRequestV1 = new ComputeRequestV1();
    String resultSchemaStr = "test_result_schema";
    computeRequestV1.resultSchemaStr = resultSchemaStr;
    computeRequestV1.operations = new LinkedList<>();

    // Add a dot-product
    String dotProductField = "dotProductField";
    String dotProductResultField = "dotProductResultField";
    List<Float> dotProductParam = new LinkedList<Float>();
    dotProductParam.add(0.1f);

    ComputeOperation dotProductOperation = new ComputeOperation();
    dotProductOperation.operationType = ComputeOperationType.DOT_PRODUCT.getValue();
    DotProduct dotProduct = (DotProduct) ComputeOperationType.DOT_PRODUCT.getNewInstance();
    dotProduct.field = dotProductField;
    dotProduct.resultFieldName = dotProductResultField;
    dotProduct.dotProductParam = dotProductParam;
    dotProductOperation.operation = dotProduct;
    computeRequestV1.operations.add(dotProductOperation);

    // Add a cosine-similarity
    String cosineSimilarityField = "cosineSimilarityField";
    String cosineSimilarityResultField = "cosineSimilarityResultField";
    List<Float> cosineSimilarityParam = new LinkedList<Float>();
    cosineSimilarityParam.add(0.2f);

    ComputeOperation cosineSimilarityOperation = new ComputeOperation();
    cosineSimilarityOperation.operationType = ComputeOperationType.COSINE_SIMILARITY.getValue();
    CosineSimilarity cosineSimilarity = (CosineSimilarity) ComputeOperationType.COSINE_SIMILARITY.getNewInstance();
    cosineSimilarity.field = cosineSimilarityField;
    cosineSimilarity.resultFieldName = cosineSimilarityResultField;
    cosineSimilarity.cosSimilarityParam = cosineSimilarityParam;
    cosineSimilarityOperation.operation = cosineSimilarity;
    computeRequestV1.operations.add(cosineSimilarityOperation);

    // serialize compute request V1
    RecordSerializer<ComputeRequestV1> computeRequestV1Serializer = SerializerDeserializerFactory
        .getAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V1.getSchema());
    byte[] serializedComputeRequestV1 = computeRequestV1Serializer.serialize(computeRequestV1);

    // use V2 schema to deserialize
    RecordDeserializer<ComputeRequestV2> computeRequestV2Deserializer = SerializerDeserializerFactory
        .getAvroSpecificDeserializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V1.getSchema(), ComputeRequestV2.class);
    ComputeRequestV2 computeRequestV2 = computeRequestV2Deserializer.deserialize(serializedComputeRequestV1);

    // check contents in the deserialized v2 record are the same as the contents in v1 record
    Assert.assertEquals(computeRequestV2.resultSchemaStr.toString(), resultSchemaStr);
    ComputeOperation computeOperation1 = (ComputeOperation) computeRequestV2.operations.get(0);
    Assert.assertEquals(computeOperation1.operationType, ComputeOperationType.DOT_PRODUCT.getValue());
    Assert.assertEquals(computeOperation1.operation, dotProduct);

    ComputeOperation computeOperation2 = (ComputeOperation) computeRequestV2.operations.get(1);
    Assert.assertEquals(computeOperation2.operationType, ComputeOperationType.COSINE_SIMILARITY.getValue());
    Assert.assertEquals(computeOperation2.operation, cosineSimilarity);
  }

  @Test
  void testToMultiGetPath() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int versionNumber = 1;
    String resourceName = storeName + "_v" + versionNumber;
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap(("key_" + i).getBytes()));
    }

    int maxKeyCount = 100;
    boolean smartLongTailRetryEnabled = false;
    int smartLongTailRetryAbortThresholdMs = -1;
    int longTailRetryMaxRouteForMultiKeyReq = 1;

    VeniceMultiGetPath multiGetPath = new VeniceMultiGetPath(
        storeName,
        versionNumber,
        resourceName,
        TestVeniceMultiGetPath.getMultiGetHttpRequest(resourceName, keys, Optional.empty()),
        getVenicePartitionFinder(-1),
        maxKeyCount,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        null,
        longTailRetryMaxRouteForMultiKeyReq,
        mock(RetryManager.class));
    byte[] serializedMultiGetRequest = multiGetPath.serializeRouterRequest();

    VeniceComputePath computePath = new VeniceComputePath(
        storeName,
        LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST,
        resourceName,
        getComputeHttpRequest(resourceName, getComputeRequest(), keys, LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST),
        getVenicePartitionFinder(-1),
        maxKeyCount,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        longTailRetryMaxRouteForMultiKeyReq,
        mock(RetryManager.class));
    VeniceMultiGetPath syntheticMultiGetPath = computePath.toMultiGetPath();
    Assert.assertEquals(syntheticMultiGetPath.serializeRouterRequest(), serializedMultiGetRequest);
    Assert
        .assertEquals(syntheticMultiGetPath.isSmartLongTailRetryEnabled(), multiGetPath.isSmartLongTailRetryEnabled());
    Assert.assertEquals(
        syntheticMultiGetPath.getSmartLongTailRetryAbortThresholdMs(),
        multiGetPath.getSmartLongTailRetryAbortThresholdMs());
    Assert.assertEquals(
        syntheticMultiGetPath.getLongTailRetryMaxRouteForMultiKeyReq(),
        multiGetPath.getLongTailRetryMaxRouteForMultiKeyReq());
  }
}
