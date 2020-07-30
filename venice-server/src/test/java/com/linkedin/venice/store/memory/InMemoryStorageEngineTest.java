package com.linkedin.venice.store.memory;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class InMemoryStorageEngineTest extends AbstractStorageEngineTest {

  StorageService service;
  VeniceStoreConfig storeConfig;
  final String STORE_NAME = "testng-in-memory";
  final int PARTITION_ID = 0;

  public InMemoryStorageEngineTest() {
  }

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() {
    if(service != null && storeConfig != null) {
      service.dropStorePartition(storeConfig , PARTITION_ID);
    }
  }

  @Override
  public void createStorageEngineForTest() {
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.IN_MEMORY);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);

    service = new StorageService(configLoader, s -> s.toString(), mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class), null);
    storeConfig = new VeniceStoreConfig(STORE_NAME, serverProperties);

    testStoreEngine = service.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
  }

  @Test
  public void testGetAndPut(){
    super.testGetAndPut();
  }

  @Test
  public void testDelete(){
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys()
  {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPartitioning()
      throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice()
      throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice()
      throws Exception{
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition()
      throws Exception {
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
