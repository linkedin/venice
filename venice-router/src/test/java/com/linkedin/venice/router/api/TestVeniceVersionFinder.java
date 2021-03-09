package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestVeniceVersionFinder {
  private final Map<String, String> clusterToD2Map = new HashMap<>();
  private final static String DEST_CLUSTER = "destCluster";
  private final static String D2_SERVICE = "d2Service";
  private final static String CLUSTER = "cluster";

  private BasicFullHttpRequest request;

  @BeforeClass
  public void setUp() {
    clusterToD2Map.put(DEST_CLUSTER, D2_SERVICE);
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/store/key", 0, 0);
  }

  @Test
  public void throws404onMissingStore(){
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    doReturn(null).when(mockRepo).getStore(anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo, getDefaultInstanceFinder(),
        stats, storeConfigRepo, clusterToD2Map, CLUSTER);
    try{
      versionFinder.getVersion("", request);
      Assert.fail("versionFinder.getVersion() on previous line should throw a "
          + VeniceNoStoreException.class.getSimpleName());
    } catch (VeniceNoStoreException e) {
      // Expected
    }
  }

  @Test
  public void throws301onMigratedStore() {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    Store store = new ZKStore("store", "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.setMigrating(true);
    int currentVersion = 10;
    store.setCurrentVersion(currentVersion);
    doReturn(store).when(mockRepo).getStore(anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig("store");
    storeConfig.setCluster(DEST_CLUSTER);
    doReturn(Optional.of(storeConfig)).when(storeConfigRepo).getStoreConfig("store");
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo, getDefaultInstanceFinder(),
        stats, storeConfigRepo, clusterToD2Map, CLUSTER);
    try {
      request.headers().add(HttpConstants.VENICE_ALLOW_REDIRECT, "1");
      versionFinder.getVersion("store", request);
      Assert.fail("versionFinder.getVersion() on previous line should throw a "
          + VeniceStoreIsMigratedException.class.getSimpleName());
    } catch (VeniceStoreIsMigratedException e) {
      Assert.assertEquals(e.getMessage(), "Store: store is migrated to cluster destCluster, d2Service d2Service");
    }
    request.headers().remove(HttpConstants.VENICE_ALLOW_REDIRECT);
    Assert.assertEquals(currentVersion, versionFinder.getVersion("store", request));
  }

  @Test
  public void returnNonExistingVersionOnceStoreIsDisabled()
      throws RouterException {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    String storeName = "TestVeniceVersionFinder";
    int currentVersion = 10;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setCurrentVersion(currentVersion);
    // disable store, should return the number indicates that none of version is avaiable to read.
    store.setEnableReads(false);
    doReturn(store).when(mockRepo).getStore(storeName);
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(mockRepo, getDefaultInstanceFinder(),
        stats, storeConfigRepo, clusterToD2Map, CLUSTER);
    try {
      versionFinder.getVersion(storeName, request);
      Assert.fail("Store should be disabled and forbidden to read.");
    } catch (StoreDisabledException e) {
      // Expected
    }
    // enable store, should return the correct current version.
    store.setEnableReads(true);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), currentVersion);
  }

  @Test
  public void onlySwapsVersionWhenAllPartitionsAreOnline() throws RouterException {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    int secondVersion = 2;
    int thirdVersion = 3;
    int fourthVersion = 4;
    int fifthVersion = 5;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();

    RoutingDataRepository routingData = mock(RoutingDataRepository.class);
    doReturn(instances).when(routingData).getReadyToServeInstances(anyString(), anyInt());
    doReturn(3).when(routingData).getNumberOfPartitions(anyString());

    PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder = mock(PartitionStatusOnlineInstanceFinder.class);
    doReturn(instances).when(partitionStatusOnlineInstanceFinder).getReadyToServeInstances(anyString(), anyInt());

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository,
        new OnlineInstanceFinderDelegator(storeRepository, routingData, partitionStatusOnlineInstanceFinder),
        stats, storeConfigRepo, clusterToD2Map, CLUSTER);

    // for a new store, the versionFinder returns the current version, no matter the online replicas
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    // When the current version changes, without any online replicas the versionFinder returns the old version number
    store.addVersion(new VersionImpl(storeName, secondVersion));
    store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(secondVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    // When we retire an old version, we update to the new version anyways
    store.addVersion(new VersionImpl(storeName, thirdVersion));
    store.updateVersionStatus(thirdVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(thirdVersion);
    store.updateVersionStatus(1, VersionStatus.NOT_CREATED);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), thirdVersion);

    // Next new version with no online instances still serves old ONLINE version
    store.addVersion(new VersionImpl(storeName, fourthVersion));
    store.updateVersionStatus(fourthVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(fourthVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), thirdVersion);

    // Once we have online replicas, the versionFinder reflects the new version
    instances.add(new Instance("id1", "host", 1234));
    Assert.assertEquals(versionFinder.getVersion(storeName, request), fourthVersion);

    // PartitionStatusOnlineInstanceFinder can also work
    store.setLeaderFollowerModelEnabled(true);
    store.addVersion(new VersionImpl(storeName, fifthVersion));
    store.updateVersionStatus(fifthVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(fifthVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), fifthVersion);
  }

  @Test
  public void returnsCurrentVersionWhenTheDictionaryExists() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    ByteBuffer firstVersionDictionary = ByteBuffer.allocate(1);

    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, Version.composeKafkaTopic(storeName, firstVersion), firstVersionDictionary.array());

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    OnlineInstanceFinderDelegator onlineInstanceFinder = mock(OnlineInstanceFinderDelegator.class);
    doReturn(3).when(onlineInstanceFinder).getNumberOfPartitions(anyString());
    doReturn(instances).when(onlineInstanceFinder).getReadyToServeInstances(anyString(), anyInt());

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository,
        onlineInstanceFinder, stats, storeConfigRepo, clusterToD2Map, CLUSTER);

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);
    Assert.assertNotNull(CompressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
  }

  @Test
  public void returnsCurrentVersionWhenItIsTheOnlyOption() {
    // When the router doesn't know of any other versions, it will return that version even if dictionary is not downloaded.
    // If the dictionary is not downloaded by the time the records needs to be decompressed, then the router will return
    // an error response.
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;

    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    OnlineInstanceFinderDelegator onlineInstanceFinder = mock(OnlineInstanceFinderDelegator.class);
    doReturn(3).when(onlineInstanceFinder).getNumberOfPartitions(anyString());
    doReturn(instances).when(onlineInstanceFinder).getReadyToServeInstances(anyString(), anyInt());

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository,
        onlineInstanceFinder, stats, storeConfigRepo, clusterToD2Map, CLUSTER);

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);
    Assert.assertNull(CompressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
  }

  @Test
  public void returnsPreviousVersionWhenDictionaryNotDownloaded() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    int secondVersion = 2;

    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    OnlineInstanceFinderDelegator onlineInstanceFinder = mock(OnlineInstanceFinderDelegator.class);
    doReturn(3).when(onlineInstanceFinder).getNumberOfPartitions(anyString());
    doReturn(instances).when(onlineInstanceFinder).getReadyToServeInstances(anyString(), anyInt());

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository,
        onlineInstanceFinder, stats, storeConfigRepo, clusterToD2Map, CLUSTER);

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
    String secondVersionKafkaTopic = Version.composeKafkaTopic(storeName, secondVersion);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    store.addVersion(new VersionImpl(storeName, secondVersion));
    store.setCurrentVersion(secondVersion);
    store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    Assert.assertNull(CompressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
    Assert.assertNull(CompressorFactory.getVersionSpecificCompressor(secondVersionKafkaTopic));
  }

  @Test
  public void returnsNewVersionWhenDictionaryDownloads() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = TestUtils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    int secondVersion = 2;

    ByteBuffer secondVersionDictionary = ByteBuffer.allocate(1);

    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    OnlineInstanceFinderDelegator onlineInstanceFinder = mock(OnlineInstanceFinderDelegator.class);
    doReturn(3).when(onlineInstanceFinder).getNumberOfPartitions(anyString());
    doReturn(instances).when(onlineInstanceFinder).getReadyToServeInstances(anyString(), anyInt());

    //Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(storeRepository,
        onlineInstanceFinder, stats, storeConfigRepo, clusterToD2Map, CLUSTER);

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
    String secondVersionKafkaTopic = Version.composeKafkaTopic(storeName, secondVersion);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    store.addVersion(new VersionImpl(storeName, secondVersion));
    store.setCurrentVersion(secondVersion);
    store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, secondVersionKafkaTopic, secondVersionDictionary.array());

    Assert.assertEquals(versionFinder.getVersion(storeName, request), secondVersion);

    Assert.assertNull(CompressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
    Assert.assertNotNull(CompressorFactory.getVersionSpecificCompressor(secondVersionKafkaTopic));
  }

  public static OnlineInstanceFinder getDefaultInstanceFinder() {
    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    RoutingDataRepository routingData = mock(RoutingDataRepository.class);
    doReturn(instances).when(routingData).getReadyToServeInstances(anyString(), anyInt());
    doReturn(3).when(routingData).getNumberOfPartitions(anyString());

    return routingData;
  }
}
