package com.linkedin.venice.router.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.Sensor;
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


public class TestVeniceVersionFinder {
  private final Map<String, String> clusterToD2Map = new HashMap<>();
  private static final String DEST_CLUSTER = "destCluster";
  private static final String D2_SERVICE = "d2Service";
  private static final String CLUSTER = "cluster";

  private BasicFullHttpRequest request;

  @BeforeClass
  public void setUp() {
    clusterToD2Map.put(DEST_CLUSTER, D2_SERVICE);
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/store/key", 0, 0);
  }

  @Test
  public void throws404onMissingStore() {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    doReturn(null).when(mockRepo).getStore(anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        mockRepo,
        getCVBasedMockedRoutingRepo(),
        stats,
        storeConfigRepo,
        clusterToD2Map,
        CLUSTER,
        compressorFactory,
        mock(VeniceMetricsRepository.class));
    try {
      versionFinder.getVersion("", request);
      Assert.fail(
          "versionFinder.getVersion() on previous line should throw a " + VeniceNoStoreException.class.getSimpleName());
    } catch (VeniceNoStoreException e) {
      // Expected
    }
  }

  @Test
  public void throws301onMigratedStore() {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    String storeName = "store";
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.setMigrating(true);
    int currentVersion = 10;
    store.setCurrentVersion(currentVersion);
    store.addVersion(new VersionImpl(storeName, currentVersion));
    doReturn(store).when(mockRepo).getStore(anyString());
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig("store");
    storeConfig.setCluster(DEST_CLUSTER);
    doReturn(Optional.of(storeConfig)).when(storeConfigRepo).getStoreConfig("store");
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceVersionFinder versionFinder = spy(
        new VeniceVersionFinder(
            mockRepo,
            getCVBasedMockedRoutingRepo(),
            stats,
            storeConfigRepo,
            clusterToD2Map,
            CLUSTER,
            compressorFactory,
            mock(VeniceMetricsRepository.class)));
    String kafkaTopicName = Version.composeKafkaTopic(storeName, currentVersion);
    doReturn(true).when(versionFinder).isPartitionResourcesReady(kafkaTopicName);
    doReturn(true).when(versionFinder).isDecompressorReady(store.getVersion(currentVersion), kafkaTopicName);
    try {
      request.headers().add(HttpConstants.VENICE_ALLOW_REDIRECT, "1");
      versionFinder.getVersion("store", request);
      Assert.fail(
          "versionFinder.getVersion() on previous line should throw a "
              + VeniceStoreIsMigratedException.class.getSimpleName());
    } catch (VeniceStoreIsMigratedException e) {
      Assert.assertEquals(e.getMessage(), "Store: store is migrated to cluster destCluster, d2Service d2Service");
    }
    request.headers().remove(HttpConstants.VENICE_ALLOW_REDIRECT);
    Assert.assertEquals(currentVersion, versionFinder.getVersion("store", request));
  }

  @Test
  public void returnNonExistingVersionOnceStoreIsDisabled() {
    ReadOnlyStoreRepository mockRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    String storeName = "TestVeniceVersionFinder";
    int currentVersion = 10;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setCurrentVersion(currentVersion);
    store.addVersion(new VersionImpl(storeName, currentVersion));
    // disable store, should return the number indicates that none of version is avaiable to read.
    store.setEnableReads(false);
    doReturn(store).when(mockRepo).getStore(storeName);
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceVersionFinder versionFinder = spy(
        new VeniceVersionFinder(
            mockRepo,
            getCVBasedMockedRoutingRepo(),
            stats,
            storeConfigRepo,
            clusterToD2Map,
            CLUSTER,
            compressorFactory,
            mock(VeniceMetricsRepository.class)));
    String kafkaTopicName = Version.composeKafkaTopic(storeName, currentVersion);
    doReturn(true).when(versionFinder).isPartitionResourcesReady(kafkaTopicName);
    doReturn(true).when(versionFinder).isDecompressorReady(store.getVersion(currentVersion), kafkaTopicName);

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
  public void testSwapsVersionWhenAllPartitionsAreOnline() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = Utils.getUniqueString("version-finder-test-store");
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

    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(instances).when(routingDataRepo).getReadyToServeInstances(anyString(), anyInt());
    doReturn(3).when(routingDataRepo).getNumberOfPartitions(anyString());
    doReturn(true).when(routingDataRepo).containsKafkaTopic(anyString());

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceMetricsRepository mockMetricsRepository = mock(VeniceMetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    // Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        storeRepository,
        routingDataRepo,
        stats,
        storeConfigRepo,
        clusterToD2Map,
        CLUSTER,
        compressorFactory,
        mockMetricsRepository);

    // for a new store, the versionFinder returns no version
    Assert.assertEquals(versionFinder.getVersion(storeName, request), Store.NON_EXISTING_VERSION);

    // When the current version changes, without any online replicas the versionFinder returns the old version number
    store.addVersion(new VersionImpl(storeName, secondVersion));
    store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(secondVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    // When we retire an old version, we return last existing version
    store.addVersion(new VersionImpl(storeName, thirdVersion));
    store.updateVersionStatus(thirdVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(thirdVersion);
    store.updateVersionStatus(1, VersionStatus.NOT_CREATED);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    // Next new version with no online instances still serves old existing version
    store.addVersion(new VersionImpl(storeName, fourthVersion));
    store.updateVersionStatus(fourthVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(fourthVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    // Once we have online replicas, the versionFinder reflects the new version
    instances.add(new Instance("id1", "host", 1234));
    Assert.assertEquals(versionFinder.getVersion(storeName, request), fourthVersion);

    // PartitionStatusOnlineInstanceFinder can also work
    store.addVersion(new VersionImpl(storeName, fifthVersion));
    store.updateVersionStatus(fifthVersion, VersionStatus.ONLINE);
    store.setCurrentVersion(fifthVersion);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), fifthVersion);
  }

  @Test
  public void returnsCurrentVersionWhenTheDictionaryExists() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = Utils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    ByteBuffer firstVersionDictionary = ByteBuffer.allocate(1);

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

    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(3).when(routingDataRepo).getNumberOfPartitions(anyString());
    doReturn(instances).when(routingDataRepo).getReadyToServeInstances(anyString(), anyInt());

    try (CompressorFactory compressorFactory = new CompressorFactory()) {
      compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.ZSTD_WITH_DICT,
          Version.composeKafkaTopic(storeName, firstVersion),
          firstVersionDictionary.array());
      // Object under test
      VeniceVersionFinder versionFinder = spy(
          new VeniceVersionFinder(
              storeRepository,
              routingDataRepo,
              stats,
              storeConfigRepo,
              clusterToD2Map,
              CLUSTER,
              compressorFactory,
              mock(VeniceMetricsRepository.class)));

      String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
      doReturn(true).when(versionFinder).isPartitionResourcesReady(firstVersionKafkaTopic);

      Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);
      Assert.assertNotNull(compressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
    }
  }

  @Test
  public void returnsNoVersionWhenNewStoreNewVersionNotReadyToServe() {
    // When the router doesn't know of any other versions and the dictionary of the existing version is not downloaded,
    // it will return no version.
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = Utils.getUniqueString("version-finder-test-store");
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

    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(3).when(routingDataRepo).getNumberOfPartitions(anyString());
    doReturn(instances).when(routingDataRepo).getReadyToServeInstances(anyString(), anyInt());

    CompressorFactory compressorFactory = mock(CompressorFactory.class);

    // Object under test
    VeniceVersionFinder versionFinder = spy(
        new VeniceVersionFinder(
            storeRepository,
            routingDataRepo,
            stats,
            storeConfigRepo,
            clusterToD2Map,
            CLUSTER,
            compressorFactory,
            mock(VeniceMetricsRepository.class)));

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
    doReturn(true).when(versionFinder).isPartitionResourcesReady(firstVersionKafkaTopic);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), Store.NON_EXISTING_VERSION);
    Assert.assertNull(compressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));

    doReturn(true).when(compressorFactory).versionSpecificCompressorExists(firstVersionKafkaTopic);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);
  }

  @Test
  public void returnsPreviousVersionWhenDictionaryNotDownloaded() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = Utils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    int secondVersion = 2;

    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setPartitionCount(3);
    store.addVersion(new VersionImpl(storeName, firstVersion));
    store.setCurrentVersion(firstVersion);
    store.updateVersionStatus(firstVersion, VersionStatus.ONLINE);
    // note: first version's compression strategy is NO_OP by default
    // -> VeniceVersionFinder::isDecompressorReady() for first version will return true

    doReturn(store).when(storeRepository).getStore(storeName);

    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);

    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(3).when(routingDataRepo).getNumberOfPartitions(anyString());
    doReturn(instances).when(routingDataRepo).getReadyToServeInstances(anyString(), anyInt());
    doReturn(true).when(routingDataRepo).containsKafkaTopic(anyString());

    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceMetricsRepository mockMetricsRepository = mock(VeniceMetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    // Object under test
    VeniceVersionFinder versionFinder = new VeniceVersionFinder(
        storeRepository,
        routingDataRepo,
        stats,
        storeConfigRepo,
        clusterToD2Map,
        CLUSTER,
        compressorFactory,
        mockMetricsRepository);

    String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
    String secondVersionKafkaTopic = Version.composeKafkaTopic(storeName, secondVersion);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
    store.addVersion(new VersionImpl(storeName, secondVersion));
    store.setCurrentVersion(secondVersion);
    store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);

    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

    Assert.assertNull(compressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
    Assert.assertNull(compressorFactory.getVersionSpecificCompressor(secondVersionKafkaTopic));
  }

  @Test
  public void returnsNewVersionWhenDictionaryDownloads() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = Utils.getUniqueString("version-finder-test-store");
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

    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(3).when(routingDataRepo).getNumberOfPartitions(anyString());
    doReturn(instances).when(routingDataRepo).getReadyToServeInstances(anyString(), anyInt());
    doReturn(true).when(routingDataRepo).containsKafkaTopic(anyString());
    VeniceMetricsRepository mockMetricsRepository = mock(VeniceMetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    try (CompressorFactory compressorFactory = new CompressorFactory()) {
      // Object under test
      VeniceVersionFinder versionFinder = new VeniceVersionFinder(
          storeRepository,
          routingDataRepo,
          stats,
          storeConfigRepo,
          clusterToD2Map,
          CLUSTER,
          compressorFactory,
          mockMetricsRepository);

      String firstVersionKafkaTopic = Version.composeKafkaTopic(storeName, firstVersion);
      String secondVersionKafkaTopic = Version.composeKafkaTopic(storeName, secondVersion);

      Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);

      store.setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT);
      store.addVersion(new VersionImpl(storeName, secondVersion));
      store.setCurrentVersion(secondVersion);
      store.updateVersionStatus(secondVersion, VersionStatus.ONLINE);

      compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.ZSTD_WITH_DICT,
          secondVersionKafkaTopic,
          secondVersionDictionary.array());

      Assert.assertEquals(versionFinder.getVersion(storeName, request), secondVersion);

      Assert.assertNull(compressorFactory.getVersionSpecificCompressor(firstVersionKafkaTopic));
      Assert.assertNotNull(compressorFactory.getVersionSpecificCompressor(secondVersionKafkaTopic));
    }
  }

  /** Since refreshOneStore() is an expensive operation, this test ensures that refreshOneStore() is only called once per store when getVersion() is called on the same store */
  @Test
  public void testRefreshOneStoreCalledOnce() {
    String storeName = Utils.getUniqueString("version-finder-test-store");
    int firstVersion = 1;
    Store store = TestUtils.createTestStore(storeName, "unittest", System.currentTimeMillis());
    store.setCurrentVersion(Store.NON_EXISTING_VERSION);

    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    HelixCustomizedViewOfflinePushRepository routingDataRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    StaleVersionStats stats = mock(StaleVersionStats.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepo = mock(HelixReadOnlyStoreConfigRepository.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VeniceMetricsRepository mockMetricsRepository = mock(VeniceMetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    VeniceVersionFinder versionFinder = spy(
        new VeniceVersionFinder(
            storeRepository,
            routingDataRepo,
            stats,
            storeConfigRepo,
            clusterToD2Map,
            CLUSTER,
            compressorFactory,
            mockMetricsRepository));

    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(store).when(storeRepository).refreshOneStore(storeName);

    // Call getVersion() multiple times while version is NON_EXISTING_VERSION
    Assert.assertEquals(versionFinder.getVersion(storeName, request), Store.NON_EXISTING_VERSION);
    Assert.assertEquals(versionFinder.getVersion(storeName, request), Store.NON_EXISTING_VERSION);

    // Verify refreshOneStore is called only once
    verify(storeRepository, times(1)).refreshOneStore(storeName);

    // Update the store's version to 1
    store.setCurrentVersion(firstVersion);
    doReturn(true).when(versionFinder).isDecompressorReady(any(), anyString());
    doReturn(true).when(versionFinder).isPartitionResourcesReady(anyString());

    // Call getVersion() again and verify it returns the updated version
    Assert.assertEquals(versionFinder.getVersion(storeName, request), firstVersion);
  }

  public static HelixCustomizedViewOfflinePushRepository getCVBasedMockedRoutingRepo() {
    List<Instance> instances = new LinkedList<>();
    instances.add(new Instance("id1", "host", 1234));

    HelixCustomizedViewOfflinePushRepository routingData = mock(HelixCustomizedViewOfflinePushRepository.class);
    doReturn(instances).when(routingData).getReadyToServeInstances(anyString(), anyInt());
    doReturn(3).when(routingData).getNumberOfPartitions(anyString());

    return routingData;
  }
}
