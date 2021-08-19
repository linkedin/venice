package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.stats.StaleVersionStats;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePathParser {
  private final Map<String, String> clusterToD2Map = new HashMap<>();
  private final static String CLUSTER = "cluster";
  private final static VeniceRouterConfig mockRouterConfig = mock(VeniceRouterConfig.class);

  VeniceVersionFinder getVersionFinder(){
    //Mock objects
    Store mockStore = mock(Store.class);
    doReturn(1).when(mockStore).getCurrentVersion();
    doReturn(true).when(mockStore).isEnableReads();
    doReturn(CompressionStrategy.NO_OP).when(mockStore).getCompressionStrategy();
    ReadOnlyStoreRepository mockMetadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockMetadataRepository).getStore(Mockito.anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    return new VeniceVersionFinder(mockMetadataRepository, TestVeniceVersionFinder.getDefaultInstanceFinder(),
        stats, storeConfigRepo, clusterToD2Map, CLUSTER, compressorFactory);
  }

  RouterStats getMockedStats() {
    RouterStats mockRouterStats = mock(RouterStats.class);
    when(mockRouterStats.getStatsByType(any())).thenReturn(mock(AggRouterHttpRequestStats.class));
    return mockRouterStats;
  }

  @BeforeClass
  public void setup() {
    RouterExceptionAndTrackingUtils.setRouterStats(new RouterStats<>( requestType -> new AggRouterHttpRequestStats(new MetricsRepository(), requestType)));
    doReturn(10).when(mockRouterConfig).getRouterMultiGetDecompressionThreads();
  }

  @AfterClass
  public void tearDown() {
    RouterExceptionAndTrackingUtils.setRouterStats(null);
  }

  @Test
  public void parsesQueries() throws RouterException {
    String uri = "storage/store/key";
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    doReturn(3).when(partitionFinder).findPartitionNumber(Mockito.anyObject(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());
    VenicePathParser parser = new VenicePathParser(getVersionFinder(), partitionFinder, getMockedStats(),
        mock(ReadOnlyStoreRepository.class), mockRouterConfig, compressorFactory);
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, 0, 0);
    VenicePath path = parser.parseResourceUri(uri, request);
    String keyB64 = Base64.getEncoder().encodeToString("key".getBytes());
    Assert.assertEquals(path.getLocation(), "storage/store_v1/3/" + keyB64 + "?f=b64");

    try {
      parser.substitutePartitionKey(path, RouterKey.fromString("key2"));
      Assert.fail("A VeniceException should be thrown when passing different key to function: substitutePartitionKey for single-get");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VeniceException);
    }
  }

  @Test
  public void parsesB64Uri() throws RouterException {
    String myUri = "/storage/storeName/bXlLZXk=?f=b64";
    String expectedKey = "myKey";
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, myUri, 0, 0);
    VenicePath path = new VenicePathParser(getVersionFinder(), partitionFinder, getMockedStats(),
        mock(ReadOnlyStoreRepository.class), mockRouterConfig, compressorFactory)
        .parseResourceUri(myUri, request);
    ByteBuffer partitionKey = path.getPartitionKey().getKeyBuffer();
    Assert.assertEquals(path.getPartitionKey().getKeyBuffer(), ByteBuffer.wrap(expectedKey.getBytes()),
        new String(partitionKey.array(), partitionKey.position(), partitionKey.remaining()) + " should match " + expectedKey);
  }

  @Test(expectedExceptions = RouterException.class)
  public void failsToParseOtherActions() throws RouterException {
    VenicePartitionFinder partitionFinder = mock(VenicePartitionFinder.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    new VenicePathParser(getVersionFinder(), partitionFinder,getMockedStats(),
         mock(ReadOnlyStoreRepository.class), mockRouterConfig, compressorFactory)
        .parseResourceUri("/badAction/storeName/key");
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
        "8startsWithNumber",
        "bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars-bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars"
    };

    for (String name : badNames){
      Assert.assertFalse(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should not be valid");
    }

  }
}
