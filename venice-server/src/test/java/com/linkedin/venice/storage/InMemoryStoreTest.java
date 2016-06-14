package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.store.memory.InMemoryStorageEngine;
import org.testng.annotations.Test;

import java.io.File;


public class InMemoryStoreTest extends AbstractStoreTest{

  VeniceConfigLoader veniceConfigLoader;

  public InMemoryStoreTest()
      throws Exception {
    createStoreForTest();
  }

  @Override
  public void createStoreForTest()
      throws Exception {
    File configFile = new File("src/test/resources/config"); //TODO this does not run from IDE because IDE expects
    // relative path starting from venice-server
    veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(configFile.getAbsolutePath());
    String storeName = "testng-in-memory";
    VeniceStoreConfig storeConfig = veniceConfigLoader.getStoreConfig(storeName);

    InMemoryStorageEngine inMemoryStorageEngine = new InMemoryStorageEngine(storeConfig);
    inMemoryStorageEngine.addStoragePartition(0);
    this.testStore = inMemoryStorageEngine;
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
  public void testPutNullKey(){
    super.testPutNullKey();
  }


}
