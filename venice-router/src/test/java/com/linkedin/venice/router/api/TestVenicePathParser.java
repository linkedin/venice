package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.StaleVersionStats;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePathParser {
  private final int TEST_MAX_KEY_COUNT_IN_MULTI_GET_REQ = 100;


  VeniceVersionFinder getVersionFinder(){
    //Mock objects
    Store mockStore = mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    doReturn(true).when(mockStore).isEnableReads();
    ReadOnlyStoreRepository mockMetadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    return new VeniceVersionFinder(mockMetadataRepository, Optional.empty(), stats);
  }

  AggRouterHttpRequestStats getMockedStats() {
    return mock(AggRouterHttpRequestStats.class);
  }

  @Test
  public void parsesQueries() throws RouterException {
    String uri = "storage/store/key";
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);
    doReturn(3).when(partitionFinder).findPartitionNumber(Mockito.anyString(), Mockito.anyObject());
    VenicePathParser parser = new VenicePathParser(getVersionFinder(), partitionFinder, getMockedStats(), getMockedStats(),
        TEST_MAX_KEY_COUNT_IN_MULTI_GET_REQ, mock(ReadOnlyStoreRepository.class), false, -1);
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, 0, 0);
    VenicePath path = parser.parseResourceUri(uri, request);
    String keyb64 = Base64.getEncoder().encodeToString("key".getBytes());
    Assert.assertEquals(path.getLocation(), "storage/store_v1/3/" + keyb64 + "?f=b64");

    try {
      parser.substitutePartitionKey(path, RouterKey.fromString("key2"));
      Assert.fail("A VeniceException should be thrown when passing different key to function: substitutePartitionKey for single-get");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VeniceException);
    }
  }

  @Test
  public void parsesB64Uri() throws RouterException {
    String myUri = "/storage/storename/bXlLZXk=?f=b64";
    String expectedKey = "myKey";
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, myUri, 0, 0);
    VenicePath path = new VenicePathParser(getVersionFinder(), partitionFinder, getMockedStats(), getMockedStats(),
        TEST_MAX_KEY_COUNT_IN_MULTI_GET_REQ, mock(ReadOnlyStoreRepository.class), false, -1)
        .parseResourceUri(myUri, request);
    ByteBuffer partitionKey = path.getPartitionKey().getKeyBuffer();
    Assert.assertEquals(path.getPartitionKey().getKeyBuffer(), ByteBuffer.wrap(expectedKey.getBytes()),
        new String(partitionKey.array(), partitionKey.position(), partitionKey.remaining()) + " should match " + expectedKey);
  }

  @Test(expectedExceptions = RouterException.class)
  public void failsToParseOtherActions() throws RouterException {
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);
    new VenicePathParser(getVersionFinder(), partitionFinder, getMockedStats(), getMockedStats(),
        TEST_MAX_KEY_COUNT_IN_MULTI_GET_REQ, mock(ReadOnlyStoreRepository.class), false, -1)
        .parseResourceUri("/badaction/storename/key");
  }

  @Test
  public void validatesResourceNames() {
    String[] goodNames = {
        "goodName",
        "good_name_with_underscores",
        "good-name-with-dashes",
        "goodNameWithNumbers1234545"
    };

    for (String name : goodNames){
      Assert.assertTrue(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should be valid");
    }

    String[] badNames = {
        "bad name with space",
        "bad.name.with.dots",
        "8startswithnumber",
        "bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars-bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars"
    };

    for (String name : badNames){
      Assert.assertFalse(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should not be valid");
    }

  }

}
