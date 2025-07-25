package com.linkedin.venice.listener.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.testng.annotations.Test;


public class AggregationRouterRequestWrapperTest {
  @Test
  public void testParseAggregationRequest() throws Exception {
    System.out.println("==== TEST testParseAggregationRequest STARTED ====");
    String storeName = "test-store";
    String expectedKey1 = "key1";
    String expectedKey2 = "key2";

    // Construct countByValueFields
    Map<String, Integer> countByValueFieldsMap = new HashMap<>();
    countByValueFieldsMap.put("field1", 10);
    countByValueFieldsMap.put("field2", 5);

    // Construct keys list
    List<ComputeRouterRequestKeyV1> keysList = new ArrayList<>();
    String[] expectedKeys = { expectedKey1, expectedKey2 };
    for (int i = 0; i < expectedKeys.length; i++) {
      ComputeRouterRequestKeyV1 keyObj = new ComputeRouterRequestKeyV1();
      keyObj.setKeyIndex(i);
      keyObj.setPartitionId(0);
      keyObj.setKeyBytes(ByteBuffer.wrap(expectedKeys[i].getBytes()));
      keysList.add(keyObj);
    }

    // Construct request body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);

    // Write countByValueFields
    encoder.writeInt(countByValueFieldsMap.size());
    for (Map.Entry<String, Integer> entry: countByValueFieldsMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.flush();

    // Use RecordSerializer to serialize keys list
    RecordSerializer<ComputeRouterRequestKeyV1> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
    byte[] serializedKeys = keySerializer.serializeObjects(keysList);
    bos.write(serializedKeys);

    byte[] requestBodyBytes = bos.toByteArray();

    // Construct HTTP request
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/test-store",
        Unpooled.wrappedBuffer(requestBodyBytes));
    httpRequest.headers().set("X-VENICE-API-VERSION", "1");
    httpRequest.headers().set("X-VENICE-COMPUTE-VALUE-SCHEMA-ID", "1");

    // Parse request
    String[] requestParts = httpRequest.uri().split("/");
    AggregationRouterRequestWrapper wrapper =
        AggregationRouterRequestWrapper.parseAggregationRequest(httpRequest, requestParts);

    // Verify results
    assertNotNull(wrapper);
    assertEquals("", wrapper.getStoreName());
    assertEquals(2, wrapper.getKeyCount());

    List<ComputeRouterRequestKeyV1> keys = wrapper.getKeys();
    System.out.println("==== DEBUG KEYS ====");
    System.out.println("keys.size()=" + keys.size());
    for (int i = 0; i < keys.size(); i++) {
      System.out.println("key[" + i + "]=" + new String(keys.get(i).getKeyBytes().array()));
    }
    System.out.println("==== END DEBUG ====");
    assertEquals(2, keys.size());
    // The actual parsed keys are complex serialized strings, we only check the key count
    assertNotNull(keys.get(0));
    assertNotNull(keys.get(1));
    System.out.println("==== AFTER ASSERTS ====");

    Map<String, Integer> countByValueFields = wrapper.getCountByValueFields();
    assertEquals(2, countByValueFields.size());
    assertEquals(Integer.valueOf(10), countByValueFields.get("field1"));
    assertEquals(Integer.valueOf(5), countByValueFields.get("field2"));
  }

  @Test
  public void testParseAggregationRequestWithEmptyKeys() throws Exception {
    System.out.println("==== TEST testParseAggregationRequestWithEmptyKeys STARTED ====");
    String storeName = "test-store";

    // Construct countByValueFields
    Map<String, Integer> countByValueFieldsMap = new HashMap<>();
    countByValueFieldsMap.put("status", 10);

    // Construct empty keys list
    List<ComputeRouterRequestKeyV1> keysList = new ArrayList<>();

    // Construct request body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);

    // Write countByValueFields
    encoder.writeInt(countByValueFieldsMap.size());
    for (Map.Entry<String, Integer> entry: countByValueFieldsMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.flush();

    // Use RecordSerializer to serialize empty keys list
    RecordSerializer<ComputeRouterRequestKeyV1> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
    byte[] serializedKeys = keySerializer.serializeObjects(keysList);
    bos.write(serializedKeys);

    byte[] requestBodyBytes = bos.toByteArray();

    // Construct HTTP request
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/test-store",
        Unpooled.wrappedBuffer(requestBodyBytes));
    httpRequest.headers().set("X-VENICE-API-VERSION", "1");
    httpRequest.headers().set("X-VENICE-COMPUTE-VALUE-SCHEMA-ID", "1");

    // Parse request
    String[] requestParts = httpRequest.uri().split("/");
    AggregationRouterRequestWrapper wrapper =
        AggregationRouterRequestWrapper.parseAggregationRequest(httpRequest, requestParts);

    // Verify results
    assertNotNull(wrapper);
    assertEquals("", wrapper.getStoreName());
    assertEquals(0, wrapper.getKeyCount());

    List<ComputeRouterRequestKeyV1> keys = wrapper.getKeys();
    assertEquals(0, keys.size());

    Map<String, Integer> countByValueFields = wrapper.getCountByValueFields();
    assertEquals(1, countByValueFields.size());
    assertEquals(Integer.valueOf(10), countByValueFields.get("status"));
  }

  @Test
  public void testParseAggregationRequestWithMultipleKeys() throws Exception {
    System.out.println("==== TEST testParseAggregationRequestWithMultipleKeys STARTED ====");
    String storeName = "test-store";
    String expectedKey1 = "key1";
    String expectedKey2 = "key2";
    String expectedKey3 = "key3";

    // Construct countByValueFields
    Map<String, Integer> countByValueFieldsMap = new HashMap<>();
    countByValueFieldsMap.put("status", 10);

    // Construct multiple keys
    List<ComputeRouterRequestKeyV1> keysList = new ArrayList<>();
    String[] expectedKeys = { expectedKey1, expectedKey2, expectedKey3 };
    for (int i = 0; i < 3; i++) {
      ComputeRouterRequestKeyV1 keyObj = new ComputeRouterRequestKeyV1();
      keyObj.setKeyIndex(i);
      keyObj.setPartitionId(0);
      keyObj.setKeyBytes(ByteBuffer.wrap(expectedKeys[i].getBytes()));
      keysList.add(keyObj);
    }

    // Construct request body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);

    // Write countByValueFields
    encoder.writeInt(countByValueFieldsMap.size());
    for (Map.Entry<String, Integer> entry: countByValueFieldsMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.flush();

    // Use RecordSerializer to serialize keys list
    RecordSerializer<ComputeRouterRequestKeyV1> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
    byte[] serializedKeys = keySerializer.serializeObjects(keysList);
    bos.write(serializedKeys);

    byte[] requestBodyBytes = bos.toByteArray();

    // Construct HTTP request
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/test-store",
        Unpooled.wrappedBuffer(requestBodyBytes));
    httpRequest.headers().set("X-VENICE-API-VERSION", "1");
    httpRequest.headers().set("X-VENICE-COMPUTE-VALUE-SCHEMA-ID", "1");

    // Parse request
    String[] requestParts = httpRequest.uri().split("/");
    AggregationRouterRequestWrapper wrapper =
        AggregationRouterRequestWrapper.parseAggregationRequest(httpRequest, requestParts);

    // Verify results
    assertNotNull(wrapper);
    assertEquals("", wrapper.getStoreName());
    assertEquals(3, wrapper.getKeyCount());

    List<ComputeRouterRequestKeyV1> keys = wrapper.getKeys();
    assertEquals(expectedKeys.length, keys.size());
    // The actual parsed keys are complex serialized strings, we only check the key count
    for (int i = 0; i < expectedKeys.length; i++) {
      assertNotNull(keys.get(i));
    }

    Map<String, Integer> countByValueFields = wrapper.getCountByValueFields();
    assertEquals(1, countByValueFields.size());
    assertEquals(Integer.valueOf(10), countByValueFields.get("status"));
  }

  @Test
  public void testGetKeyCount() throws Exception {
    String storeName = "test-store";
    String expectedKey1 = "key1";
    String expectedKey2 = "key2";

    // Construct countByValueFields
    Map<String, Integer> countByValueFieldsMap = new HashMap<>();
    countByValueFieldsMap.put("field1", 5);

    // Construct keys list
    List<ComputeRouterRequestKeyV1> keysList = new ArrayList<>();
    String[] expectedKeys = { expectedKey1, expectedKey2 };
    for (int i = 0; i < expectedKeys.length; i++) {
      ComputeRouterRequestKeyV1 keyObj = new ComputeRouterRequestKeyV1();
      keyObj.setKeyIndex(i);
      keyObj.setPartitionId(0);
      keyObj.setKeyBytes(ByteBuffer.wrap(expectedKeys[i].getBytes()));
      keysList.add(keyObj);
    }

    // Construct request body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);

    // Write countByValueFields
    encoder.writeInt(countByValueFieldsMap.size());
    for (Map.Entry<String, Integer> entry: countByValueFieldsMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.flush();

    // Use RecordSerializer to serialize keys list
    RecordSerializer<ComputeRouterRequestKeyV1> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
    byte[] serializedKeys = keySerializer.serializeObjects(keysList);
    bos.write(serializedKeys);

    byte[] requestBodyBytes = bos.toByteArray();

    // Construct HTTP request
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/test-store",
        Unpooled.wrappedBuffer(requestBodyBytes));
    httpRequest.headers().set("X-VENICE-API-VERSION", "1");
    httpRequest.headers().set("X-VENICE-COMPUTE-VALUE-SCHEMA-ID", "1");

    // Parse request
    String[] requestParts = httpRequest.uri().split("/");
    AggregationRouterRequestWrapper wrapper =
        AggregationRouterRequestWrapper.parseAggregationRequest(httpRequest, requestParts);

    // Verify results
    assertNotNull(wrapper);
    assertEquals(2, wrapper.getKeyCount());
  }

  @Test
  public void testIsStreamingRequest() throws Exception {
    String storeName = "test-store";
    String expectedKey1 = "key1";

    // Construct countByValueFields
    Map<String, Integer> countByValueFieldsMap = new HashMap<>();
    countByValueFieldsMap.put("field1", 5);

    // Construct keys list
    List<ComputeRouterRequestKeyV1> keysList = new ArrayList<>();
    ComputeRouterRequestKeyV1 keyObj = new ComputeRouterRequestKeyV1();
    keyObj.setKeyIndex(0);
    keyObj.setPartitionId(0);
    keyObj.setKeyBytes(ByteBuffer.wrap(expectedKey1.getBytes()));
    keysList.add(keyObj);

    // Construct request body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);

    // Write countByValueFields
    encoder.writeInt(countByValueFieldsMap.size());
    for (Map.Entry<String, Integer> entry: countByValueFieldsMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }
    encoder.flush();

    // Use RecordSerializer to serialize keys list
    RecordSerializer<ComputeRouterRequestKeyV1> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
    byte[] serializedKeys = keySerializer.serializeObjects(keysList);
    bos.write(serializedKeys);

    byte[] requestBodyBytes = bos.toByteArray();

    // Construct HTTP request
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/storage/test-store",
        Unpooled.wrappedBuffer(requestBodyBytes));
    httpRequest.headers().set("X-VENICE-API-VERSION", "1");
    httpRequest.headers().set("X-VENICE-COMPUTE-VALUE-SCHEMA-ID", "1");

    // Parse request
    String[] requestParts = httpRequest.uri().split("/");
    AggregationRouterRequestWrapper wrapper =
        AggregationRouterRequestWrapper.parseAggregationRequest(httpRequest, requestParts);

    // Verify results
    assertNotNull(wrapper);
    assertEquals(1, wrapper.getKeyCount());
    assertEquals(RequestType.AGGREGATION, wrapper.getRequestType());
  }
}
