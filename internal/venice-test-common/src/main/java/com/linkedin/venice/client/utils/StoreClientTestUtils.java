package com.linkedin.venice.client.utils;

import static org.mockito.Mockito.doReturn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreClientTestUtils {
  private static Logger LOGGER = LogManager.getLogger(StoreClientTestUtils.class);

  public static FullHttpResponse constructHttpSchemaResponse(String storeName, int schemaId, String schemaStr)
      throws IOException {
    ByteBuf body = Unpooled.wrappedBuffer(constructSchemaResponseInBytes(storeName, schemaId, schemaStr));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static FullHttpResponse constructHttpClusterDiscoveryResponse(
      String storeName,
      String clusterName,
      String d2Service) throws IOException {
    D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setD2Service(d2Service);
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    byte[] bytes = mapper.writeValueAsBytes(responseObject);
    ByteBuf body = Unpooled.wrappedBuffer(bytes);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static byte[] constructSchemaResponseInBytes(String storeName, int schemaId, String schemaStr)
      throws IOException {
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster("test_cluster");
    responseObject.setName(storeName);
    responseObject.setId(schemaId);
    responseObject.setSchemaStr(schemaStr);
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    return mapper.writeValueAsBytes(responseObject);
  }

  public static FullHttpResponse constructHttpMultiSchemaResponse(
      String storeName,
      Map<Integer, String> valueSchemaEntries) throws IOException {
    ByteBuf body = Unpooled.wrappedBuffer(constructMultiSchemaResponseInBytes(storeName, valueSchemaEntries));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static FullHttpResponse constructHttpMultiSchemaIdResponse(
      String storeName,
      Map<Integer, String> valueSchemaEntries) throws IOException {
    ByteBuf body = Unpooled.wrappedBuffer(constructMultiSchemaIdResponseInBytes(storeName, valueSchemaEntries));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static byte[] constructMultiSchemaIdResponseInBytes(String storeName, Map<Integer, String> valueSchemaEntries)
      throws IOException {
    MultiSchemaIdResponse responseObject = new MultiSchemaIdResponse();
    responseObject.setCluster("test_cluster");
    responseObject.setName(storeName);
    responseObject.setSchemaIdSet(valueSchemaEntries.keySet());
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    return mapper.writeValueAsBytes(responseObject);
  }

  public static byte[] constructMultiSchemaResponseInBytes(String storeName, Map<Integer, String> valueSchemaEntries)
      throws IOException {
    MultiSchemaResponse responseObject = new MultiSchemaResponse();
    responseObject.setCluster("test_cluster");
    responseObject.setName(storeName);
    int schemaNum = valueSchemaEntries.size();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
    int cur = 0;
    for (Map.Entry<Integer, String> entry: valueSchemaEntries.entrySet()) {
      schemas[cur] = new MultiSchemaResponse.Schema();
      schemas[cur].setId(entry.getKey());
      schemas[cur].setSchemaStr(entry.getValue());
      ++cur;
    }
    responseObject.setSchemas(schemas);
    ObjectMapper mapper = ObjectMapperFactory.getInstance();

    return mapper.writeValueAsBytes(responseObject);
  }

  public static FullHttpResponse constructStoreResponse(int schemaId, byte[] value) throws IOException {
    ByteBuf body = Unpooled.wrappedBuffer(value);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, Integer.toString(schemaId));
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static byte[] serializeRecord(Object object, Schema schema) throws VeniceClientException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(output, true, null);
    GenericDatumWriter<Object> datumWriter = null;
    try {
      datumWriter = new GenericDatumWriter<>(schema);
      datumWriter.write(object, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new VeniceClientException("Could not serialize the Avro object" + e);
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close stream", e);
        }
      }
    }
    return output.toByteArray();
  }

  public static void setupSchemaResponse(TransportClient transportClient, byte[] response, String path) {
    CompletableFuture<TransportClientResponse> transportFuture = new CompletableFuture<>();
    transportFuture.complete(new TransportClientResponse(-1, CompressionStrategy.NO_OP, response));
    doReturn(transportFuture).when(transportClient).get(path);
  }

  public static void setupMultiValueSchemaResponse(
      TransportClient transportClient,
      String storeName,
      Map<Integer, Schema> valueSchemas) throws IOException {
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemas.forEach((schemaId, schema) -> valueSchemaEntries.put(schemaId, schema.toString()));
    byte[] multiSchemaResponseInBytes =
        StoreClientTestUtils.constructMultiSchemaIdResponseInBytes(storeName, valueSchemaEntries);
    setupSchemaResponse(
        transportClient,
        multiSchemaResponseInBytes,
        RouterBackedSchemaReader.TYPE_ALL_VALUE_SCHEMA_IDS + "/" + storeName);
    valueSchemaEntries.forEach((schemaId, schema) -> {
      byte[] singleSchemaResponseInBytes = null;
      try {
        singleSchemaResponseInBytes = StoreClientTestUtils.constructSchemaResponseInBytes(storeName, schemaId, schema);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      setupSchemaResponse(
          transportClient,
          singleSchemaResponseInBytes,
          RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + schemaId);
    });
  }
}
