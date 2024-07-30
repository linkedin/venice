package com.linkedin.venice.router.api.path;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.router.RouterThrottleHandler;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceMultiGetPath {
  @BeforeClass
  public void setUp() {
    RouterExceptionAndTrackingUtils.setRouterStats(
        new RouterStats<>(
            requestType -> new AggRouterHttpRequestStats(
                new MetricsRepository(),
                requestType,
                mock(ReadOnlyStoreRepository.class),
                true)));
  }

  @AfterClass
  public void cleanUp() {
    RouterExceptionAndTrackingUtils.setRouterStats(null);
  }

  private static byte[] serializeKeys(Iterable<ByteBuffer> keys) {
    RecordSerializer<ByteBuffer> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(VeniceMultiGetPath.EXPECTED_PROTOCOL.getSchema());
    return serializer.serializeObjects(keys);
  }

  private VenicePartitionFinder getVenicePartitionFinder(int partitionId) {
    VenicePartitionFinder mockedPartitionFinder = mock(VenicePartitionFinder.class);
    VenicePartitioner venicePartitioner = mock(VenicePartitioner.class);
    when(venicePartitioner.getPartitionId(any(ByteBuffer.class), anyInt())).thenReturn(partitionId);
    when(mockedPartitionFinder.findPartitioner(any(), anyInt())).thenReturn(venicePartitioner);
    return mockedPartitionFinder;
  }

  static BasicFullHttpRequest getMultiGetHttpRequest(
      String resourceName,
      List<ByteBuffer> keys,
      Optional<Integer> apiVersion) {
    String uri = "/storage/" + resourceName;
    byte[] contentBytes = serializeKeys(keys);

    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        uri,
        Unpooled.wrappedBuffer(contentBytes),
        0,
        0);
    if (apiVersion.isPresent()) {
      request.headers().add(HttpConstants.VENICE_API_VERSION, apiVersion.get());
    } else {
      request.headers()
          .add(HttpConstants.VENICE_API_VERSION, VeniceMultiGetPath.EXPECTED_PROTOCOL.getProtocolVersion());
    }

    return request;
  }

  @Test(expectedExceptions = VeniceKeyCountLimitException.class, expectedExceptionsMessageRegExp = ".*exceeds key count limit.*")
  public void testMultiGetReqWithTooManyKeys() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;

    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    RecordSerializer<ByteBuffer> multiGetRequestSerializer = SerializerDeserializerFactory
        .getAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
    BasicFullHttpRequest request = getMultiGetHttpRequest(resourceName, keys, Optional.empty());
    request.attr(RouterThrottleHandler.THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY)
        .set(multiGetRequestSerializer.serializeObjects(keys));
    new VeniceMultiGetPath(
        storeName,
        version,
        resourceName,
        request,
        getVenicePartitionFinder(-1),
        3,
        false,
        -1,
        null,
        1);
  }

  @Test(expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*but received.*")
  public void testMultiGetReqWithInvalidAPIVersion() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    BasicFullHttpRequest request = getMultiGetHttpRequest(
        resourceName,
        keys,
        Optional.of(VeniceMultiGetPath.EXPECTED_PROTOCOL.getProtocolVersion() + 1));

    new VeniceMultiGetPath(
        storeName,
        version,
        resourceName,
        request,
        getVenicePartitionFinder(-1),
        3,
        false,
        -1,
        null,
        1);
  }

  @Test
  public void testAllowedRouteRetry() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;

    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    RecordSerializer<ByteBuffer> multiGetRequestSerializer = SerializerDeserializerFactory
        .getAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
    BasicFullHttpRequest request = getMultiGetHttpRequest(resourceName, keys, Optional.empty());
    request.attr(RouterThrottleHandler.THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY)
        .set(multiGetRequestSerializer.serializeObjects(keys));

    VenicePath path = new VeniceMultiGetPath(
        storeName,
        version,
        resourceName,
        request,
        getVenicePartitionFinder(-1),
        10,
        false,
        -1,
        null,
        1);
    Assert.assertTrue(path.isLongTailRetryAllowedForNewRoute());
    Assert.assertFalse(path.isLongTailRetryAllowedForNewRoute());

    request = getMultiGetHttpRequest(resourceName, keys, Optional.empty());
    request.attr(RouterThrottleHandler.THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY)
        .set(multiGetRequestSerializer.serializeObjects(keys));
    path = new VeniceMultiGetPath(
        storeName,
        version,
        resourceName,
        request,
        getVenicePartitionFinder(-1),
        10,
        false,
        -1,
        null,
        -1);
    Assert.assertTrue(path.isLongTailRetryAllowedForNewRoute());
    Assert.assertTrue(path.isLongTailRetryAllowedForNewRoute());
  }
}
