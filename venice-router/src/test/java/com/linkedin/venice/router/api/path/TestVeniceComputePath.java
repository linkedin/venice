package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestVeniceComputePath {
  private static String resultSchemaStr = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },       " +
      "         { \"name\": \"member_score\", \"type\": \"double\" }        " +
      "  ]       " +
      " }       ";


  private ComputeRequestV1 getComputeRequest() {
    DotProduct dotProduct = new DotProduct();
    dotProduct.field = "member_feature";
    List<Float> featureVector = new ArrayList<>(3);
    featureVector.add(Float.valueOf((float)0.4));
    featureVector.add(Float.valueOf((float)66.6));
    featureVector.add(Float.valueOf((float)5.20));
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

  private BasicFullHttpRequest getComputeHttpRequest(String resourceName, byte[] content) {
    String uri = "/compute/" + resourceName;

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri,
        Unpooled.wrappedBuffer(content),0, 0);
    request.headers().add(HttpConstants.VENICE_API_VERSION, VeniceComputePath.EXPECTED_PROTOCOL.getProtocolVersion());

    return request;
  }

  private VenicePartitionFinder getVenicePartitionFinder(int partitionId) {
    VenicePartitionFinder mockedPartitionFinder = mock(VenicePartitionFinder.class);
    doReturn(partitionId).when(mockedPartitionFinder).findPartitionNumber(any(), any());
    return mockedPartitionFinder;
  }

  @Test
  public void testDeserializationCorrectness() throws RouterException {
    String resourceName = TestUtils.getUniqueString("test_store") + "_v1";

    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    ComputeRequestV1 computeRequest = getComputeRequest();

    RecordSerializer<ComputeRequestV1> computeRequestSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRequestV1.SCHEMA$);
    byte[] computeRequestInBytes = computeRequestSerializer.serialize(computeRequest);
    int expectedLength = computeRequestInBytes.length;

    RecordSerializer<ByteBuffer> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());
    byte[] keysInBytes = keySerializer.serializeObjects(keys);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      output.write(computeRequestInBytes);
      output.write(keysInBytes);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Failed to write bytes to output stream", e);
    }

    BasicFullHttpRequest request = getComputeHttpRequest(resourceName, output.toByteArray());

    VeniceComputePath computePath = new VeniceComputePath(resourceName, request,
        getVenicePartitionFinder(-1), 10, false, -1);
    Assert.assertEquals(computePath.getComputeRequestLengthInBytes(), expectedLength);

    ComputeRequestV1 requestInPath = computePath.getComputeRequest();
    Schema resultSchemaInPath = Schema.parse(requestInPath.resultSchemaStr.toString());
    Schema expectedResultSchema = Schema.parse(computeRequest.resultSchemaStr.toString());

    Assert.assertTrue(resultSchemaInPath.equals(expectedResultSchema));
    Assert.assertEquals(requestInPath.operations, computeRequest.operations);
  }
}
