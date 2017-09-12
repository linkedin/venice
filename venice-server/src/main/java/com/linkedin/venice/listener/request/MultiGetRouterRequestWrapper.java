package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class MultiGetRouterRequestWrapper extends RouterRequest {
  private final Iterable<MultiGetRouterRequestKeyV1> keys;
  private int keyCount = 0;

  private MultiGetRouterRequestWrapper(String resourceName, Iterable<MultiGetRouterRequestKeyV1> keys) {
    super(resourceName);

    this.keys = keys;
    this.keys.forEach( key -> ++keyCount);
  }

  public static MultiGetRouterRequestWrapper parseMultiGetHttpRequest(FullHttpRequest httpRequest) {
    URI fullUri = URI.create(httpRequest.uri());
    String path = fullUri.getRawPath();
    String[] requestParts = path.split("/");
    if (requestParts.length != 3) { // [0]""/[1]"storage"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + path);
    }
    String resourceName = requestParts[2];

    // Validate API version
    String apiVersion = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (null == apiVersion) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    int expectedApiVersion = ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion();
    if (Integer.parseInt(apiVersion) != expectedApiVersion) {
      throw new VeniceException("Expected API version: " + expectedApiVersion + ", but received: " + apiVersion);
    }
    Iterable<MultiGetRouterRequestKeyV1> keys;

    try (InputStream is = new ByteBufInputStream(httpRequest.content())) {
      keys = parseKeys(is);
    } catch (IOException e) {
      throw new VeniceException("Failed to close ByteBufInputStream", e);
    }

    return new MultiGetRouterRequestWrapper(resourceName, keys);
  }

  public static Iterable<MultiGetRouterRequestKeyV1> parseKeys(InputStream content) {
    RecordDeserializer<MultiGetRouterRequestKeyV1> deserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.class);

    return deserializer.deserializeObjects(content);
  }

  public Iterable<MultiGetRouterRequestKeyV1> getKeys() {
    return this.keys;
  }

  public String toString() {
    return "MultiGetRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  public int getKeyCount() {
    return this.keyCount;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }
}
