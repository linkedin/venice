package com.linkedin.davinci.store.memory;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngineAccessor;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class InMemoryStorageEngineTest extends AbstractStorageEngineTest<InMemoryStorageEngine> {
  StorageService service;
  VeniceStoreVersionConfig storeConfig;
  final static String STORE_NAME = "testng-in-memory";
  final static int PARTITION_ID = 0;

  public InMemoryStorageEngineTest() {
  }

  @BeforeClass
  public void setUp() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void cleanUp() {
    if (service != null && storeConfig != null) {
      service.dropStorePartition(storeConfig, PARTITION_ID);
    }
  }

  @Override
  public void createStorageEngineForTest() {
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.IN_MEMORY);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);

    service = new StorageService(
        configLoader,
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mock(ReadOnlyStoreRepository.class));
    storeConfig = new VeniceStoreVersionConfig(STORE_NAME, serverProperties);

    testStoreEngine = StorageEngineAccessor
        .getInnerStorageEngine(service.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null));
    createStoreForTest();
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testGetByKeyPrefixManyKeys() {
    super.testGetByKeyPrefixManyKeys();
  }

  @Test
  public void testGetByKeyPrefixMaxSignedByte() {
    super.testGetByKeyPrefixMaxSignedByte();
  }

  @Test
  public void testGetByKeyPrefixMaxUnsignedByte() {
    super.testGetByKeyPrefixMaxUnsignedByte();
  }

  @Test
  public void testGetByKeyPrefixByteOverflow() {
    super.testGetByKeyPrefixByteOverflow();
  }

  @Test
  public void testDelete() {
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys() {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPartitioning() throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice() throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice() throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition() throws Exception {
    super.testOperationsOnNonExistingPartition();
  }

  /**
   * This test defined in {@link AbstractStorageEngineTest} doesn't work for {@link InMemoryStorageEngine}.
   */
  @Test
  public void testAdjustStoragePartitionFromTransactionalToDeferredWrite() {
  }

  /**
   * This test defined in {@link AbstractStorageEngineTest} doesn't work for {@link InMemoryStorageEngine}.
   */
  @Test
  public void testAdjustStoragePartitionFromDeferredWriteToTransactional() {
  }
}
