package com.linkedin.venice.client.utils;

import com.google.common.net.HttpHeaders;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceMessageException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class StoreClientTestUtils {
  private static Logger LOGGER = Logger.getLogger(StoreClientTestUtils.class);

  public static FullHttpResponse constructSchemaResponse(String storeName, int schemaId, String schemaStr) throws IOException {
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster("test_cluster");
    responseObject.setName(storeName);
    responseObject.setId(1);
    responseObject.setSchemaStr(schemaStr);
    ObjectMapper mapper = new ObjectMapper();
    ByteBuf body = Unpooled.wrappedBuffer(mapper.writeValueAsBytes(responseObject));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaders.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaders.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static FullHttpResponse constructMultiSchemaResponse(String storeName, Map<Integer, String> valueSchemaEntries) throws IOException {
    MultiSchemaResponse responseObject = new MultiSchemaResponse();
    responseObject.setCluster("test_cluster");
    responseObject.setName(storeName);
    int schemaNum = valueSchemaEntries.size();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
    int cur = 0;
    for (Map.Entry<Integer, String> entry : valueSchemaEntries.entrySet()) {
      schemas[cur] = new MultiSchemaResponse.Schema();
      schemas[cur].setId(entry.getKey());
      schemas[cur].setSchemaStr(entry.getValue());
      ++cur;
    }
    responseObject.setSchemas(schemas);
    ObjectMapper mapper = new ObjectMapper();
    ByteBuf body = Unpooled.wrappedBuffer(mapper.writeValueAsBytes(responseObject));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaders.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaders.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static FullHttpResponse constructStoreResponse(int schemaId, byte[] value) throws IOException {
    ByteBuf body = Unpooled.wrappedBuffer(value);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(HttpHeaders.CONTENT_TYPE, "text/plain");
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, Integer.toString(schemaId));
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaders.CONTENT_LENGTH, response.content().readableBytes());

    return response;
  }

  public static byte[] serializeRecord(Object object, Schema schema) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(output);
    GenericDatumWriter<Object> datumWriter = null;
    try {
      datumWriter = new GenericDatumWriter<>(schema);
      datumWriter.write(object, encoder);
      encoder.flush();
    } catch(IOException e) {
      throw new VeniceMessageException("Could not serialize the Avro object" + e);
    } finally {
      if(output != null) {
        try {
          output.close();
        } catch(IOException e) {
          LOGGER.error("Failed to close stream", e);
        }
      }
    }
    return output.toByteArray();
  }
}
