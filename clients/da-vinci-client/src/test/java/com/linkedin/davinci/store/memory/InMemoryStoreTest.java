package com.linkedin.davinci.store.memory;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.store.AbstractStoreTest;
import java.io.File;
import org.testng.annotations.Test;


public class InMemoryStoreTest extends AbstractStoreTest {
  VeniceConfigLoader veniceConfigLoader;

  public InMemoryStoreTest() throws Exception {
    createStoreForTest();
  }

  @Override
  public final void createStoreForTest() throws Exception {
    File configFile = new File("src/test/resources/config"); // TODO this does not run from IDE because IDE expects
    // relative path starting from venice-server
    veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(configFile.getAbsolutePath());
    String storeName = "testng-in-memory";
    VeniceStoreVersionConfig storeConfig = veniceConfigLoader.getStoreConfig(storeName);

    InMemoryStorageEngine inMemoryStorageEngine = new InMemoryStorageEngine(storeConfig);
    inMemoryStorageEngine.addStoragePartitionIfAbsent(0);
    this.testStore = inMemoryStorageEngine;
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
}
