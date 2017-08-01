package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import io.netty.buffer.ByteBufInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.log4j.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceMultiGetPath extends VenicePath {
  private static final Logger LOGGER = Logger.getLogger(VeniceMultiGetPath.class);
  public static final ReadAvroProtocolDefinition EXPECTED_PROTOCOL = ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1;

  private final SortedMap<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap;

  public VeniceMultiGetPath(String resourceName, BasicFullHttpRequest request, VenicePartitionFinder partitionFinder,
      int maxKeyCount)
      throws RouterException {
    super(resourceName);

    // Validate API version
    int apiVersion = Integer.parseInt(request.headers().get(HttpConstants.VENICE_API_VERSION));
    if (apiVersion != EXPECTED_PROTOCOL.getProtocolVersion()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST, "Expected api version: " + EXPECTED_PROTOCOL.getProtocolVersion() + ", but received: " + apiVersion);
    }

    this.routerKeyMap = new TreeMap<>();
    Iterable<ByteBuffer> keys = null;
    try (InputStream is = new ByteBufInputStream(request.content())) {
      keys = deserialize(is);
    } catch (IOException ioe) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          INTERNAL_SERVER_ERROR, "Failed to close ByteBufInputStream: " + ioe.getMessage());
    }

    int keyIdx = 0;
    for (ByteBuffer key : keys) {
      byte[] keyBytes = key.array();
      RouterKey routerKey = RouterKey.fromBytes(keyBytes);
      if (this.routerKeyMap.containsKey(routerKey)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
            BAD_REQUEST, "Multi-get request contains duplicate key, store name: " + getStoreName());
      }
      // partition lookup
      int partitionId = partitionFinder.findPartitionNumber(resourceName, routerKey);
      MultiGetRouterRequestKeyV1 routerRequestKey = new MultiGetRouterRequestKeyV1();
      routerRequestKey.keyBytes = key;
      routerRequestKey.keyIndex = keyIdx;
      routerRequestKey.partitionId = partitionId;
      this.routerKeyMap.put(routerKey, routerRequestKey);
      ++keyIdx;
    }
    setPartitionKeys(this.routerKeyMap.keySet());

    int keyCount = getPartitionKeys().size();
    if (keyCount > maxKeyCount) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST, "Key count in multi-get request exceeds the threshold: " + maxKeyCount);
    }
  }

  private VeniceMultiGetPath(String resourceName, SortedMap<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap) {
    super(resourceName);
    this.routerKeyMap = routerKeyMap;
    setPartitionKeys(routerKeyMap.keySet());
  }

  @Nonnull
  @Override
  public String getLocation() {
    String sep = VenicePathParser.SEP;
    StringBuilder sb = new StringBuilder();
    sb.append(VenicePathParser.TYPE_STORAGE).append(sep)
        .append(getResourceName());
    return sb.toString();
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} for the given key, and the {@link VeniceMultiGetPath}
   * generated will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(RouterKey s) {
    if (!routerKeyMap.containsKey(s)) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_GATEWAY, "RouterKey: " + s + " should exist in the original path");
    }
    SortedMap<RouterKey, MultiGetRouterRequestKeyV1> newRouterKeyMap = new TreeMap<>();
    newRouterKeyMap.put(s, routerKeyMap.get(s));

    return new VeniceMultiGetPath(getResourceName(), newRouterKeyMap);
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} for the given key subset, and the {@link VeniceMultiGetPath}
   * generated will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    SortedMap<RouterKey, MultiGetRouterRequestKeyV1> newRouterKeyMap = new TreeMap<>();
    for (RouterKey key : s) {
      if (!routerKeyMap.containsKey(key)) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
            BAD_GATEWAY, "RouterKey: " + s + " should exist in the original path");
      }
      newRouterKeyMap.put(key, routerKeyMap.get(key));
    }

    return new VeniceMultiGetPath(getResourceName(), newRouterKeyMap);
  }

  @Override
  public HttpUriRequest composeRouterRequest(String storageNodeUri) {
    HttpPost routerRequest = new HttpPost(storageNodeUri + getLocation());
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(serializedMultiGetRequest()));
    routerRequest.setEntity(entity);

    // Setup API version header
    routerRequest.setHeader(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));

    return routerRequest;
  }

  private byte[] serializedMultiGetRequest() {
    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        AvroSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);

    return serializer.serializeObjects(routerKeyMap.values());
  }

  private static Iterable<ByteBuffer> deserialize(InputStream is) {
    RecordDeserializer<ByteBuffer> deserializer = AvroSerializerDeserializerFactory.getAvroGenericDeserializer(
        EXPECTED_PROTOCOL.getSchema());
    return deserializer.deserializeObjects(is);
  }
}
