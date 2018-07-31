package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestVeniceMultiGetPath {

  private byte[] serializeKeys(Iterable<ByteBuffer> keys) {
    RecordSerializer<ByteBuffer> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(VeniceMultiGetPath.EXPECTED_PROTOCOL.getSchema());
    return serializer.serializeObjects(keys);
  }

  private VenicePartitionFinder getVenicePartitionFinder(int partitionId) {
    VenicePartitionFinder mockedPartitionFinder = mock(VenicePartitionFinder.class);
    doReturn(partitionId).when(mockedPartitionFinder).findPartitionNumber(any(), any());
    return mockedPartitionFinder;
  }

  private BasicFullHttpRequest getMultiGetHttpRequest(String resourceName, List<ByteBuffer> keys, Optional<Integer> apiVersion) {
    String uri = "/storage/" + resourceName;
    byte[] contentBytes = serializeKeys(keys);

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri,
        Unpooled.wrappedBuffer(contentBytes),0, 0);
    if (apiVersion.isPresent()) {
      request.headers().add(HttpConstants.VENICE_API_VERSION, apiVersion.get());
    } else {
      request.headers().add(HttpConstants.VENICE_API_VERSION, VeniceMultiGetPath.EXPECTED_PROTOCOL.getProtocolVersion());
    }

    return request;
  }

  @Test (expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*exceeds the threshold.*")
  public void testMultiGetReqWithTooManyKeys() throws RouterException {
    String resourceName = TestUtils.getUniqueString("test_store") + "_v1";

    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    BasicFullHttpRequest request = getMultiGetHttpRequest(resourceName, keys, Optional.empty());

    new VeniceMultiGetPath(resourceName, request, getVenicePartitionFinder(-1), 3);
  }

  @Test (expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*but received.*")
  public void testMultiGetReqWithInvalidAPIVersion() throws RouterException {
    String resourceName = TestUtils.getUniqueString("test_store") + "_v1";
    String keyPrefix = "key_";
    List<ByteBuffer> keys = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      keys.add(ByteBuffer.wrap((keyPrefix + i).getBytes()));
    }
    BasicFullHttpRequest request = getMultiGetHttpRequest(resourceName, keys, Optional.of(VeniceMultiGetPath.EXPECTED_PROTOCOL.getProtocolVersion() + 1));

    new VeniceMultiGetPath(resourceName, request, getVenicePartitionFinder(-1), 3);
  }
}
