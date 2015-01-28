package com.linkedin.venice.storage;

import com.linkedin.venice.Common.TestUtils;
import com.linkedin.venice.store.Store;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test the operations of the Store Interface
 * 1. put, get, delete and overwriting put operations
 * 2. Test non existent Keys
 * 3. Test null keys
 * 4.
 */
public abstract class AbstractStoreTest {

  protected Store testStore;
  protected int numOfPartitions;
  int keySize = 50;
  int valueSize = 500;
  int uniqueKeyOrValueSize = 350;

  public abstract void createStoreForTest();

  protected byte[] doGet(int partitionId, byte[] key)
      throws VeniceStorageException {
    byte[] result;
    result = testStore.get(partitionId, key);
    return result;
  }

  protected void doPut(int partitionId, byte[] key, byte[] value)
      throws VeniceStorageException {
    testStore.put(partitionId, key, value);
  }

  protected void doDelete(int partitionId, byte[] key)
      throws VeniceStorageException {
    testStore.delete(partitionId, key);
  }

  @Test
  public void testGetAndPut() {
    byte[] key = TestUtils.getRandomBytes(keySize);
    byte[] value = TestUtils.getRandomBytes(valueSize);
    int partitionId = TestUtils.getRandomIntInRange(numOfPartitions);
    byte[] foundValue;
    try {
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(value, foundValue, "The actual value: " + value.toString() + " and expected value: " + foundValue.toString() + " do not match!");
    } catch (VeniceStorageException e) {
      Assert.fail("VeniceStorageException was thrown: " + e.getMessage(), e);
    }
  }

  @Test
  public void testDelete() {
    byte[] key = TestUtils.getRandomBytes(keySize);
    byte[] value = TestUtils.getRandomBytes(valueSize);
    int partitionId = TestUtils.getRandomIntInRange(numOfPartitions);
    byte[] foundValue;
    try {
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(value, foundValue, "The actual value: " + value.toString() + " and expected value: " + foundValue.toString() + " do not match!");
      doDelete(partitionId, key);
      foundValue = doGet(partitionId,
          key); // TODO Check for an InvalidException here later. For now null is returned if key doesn't exist
      if(foundValue != null){
        Assert.fail("Delete failed. found a value: " + foundValue.toString() + "  for the key: " + key.toString() + " after deletion. ");
      }
    } catch (VeniceStorageException e) {
      Assert.fail("VeniceStorageException was thrown: " + e.getMessage(), e);
    }

  }

  @Test
  public void testUpdate() {
    byte[] key = TestUtils.getRandomBytes(keySize);
    byte[] value = TestUtils.getRandomBytes(valueSize);
    byte[] updatedValue = TestUtils.getRandomBytes(uniqueKeyOrValueSize);
    int partitionId = TestUtils.getRandomIntInRange(numOfPartitions);
    byte[] foundValue;
    try{
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(value, foundValue, "The actual value: " + value.toString() + " and expected value: " + foundValue.toString() + " do not match!");
      doPut(partitionId, key, updatedValue);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(updatedValue, foundValue, "The updated value: " + updatedValue.toString() + " and expected value: " + foundValue.toString() + " do not match!");
    } catch (VeniceStorageException e) {
      Assert.fail("VeniceStorageException was thrown: " + e.getMessage(), e);
    }
  }

  @Test
  public void testGetInvalidKeys() {
    byte[] key = TestUtils.getRandomBytes(uniqueKeyOrValueSize);
    int partitionId = TestUtils.getRandomIntInRange(numOfPartitions);
    byte[] foundValue;
    try{
      foundValue = doGet(partitionId,key); // TODO Check for an InvalidException here later. For now null is returned if key doesn't exist
      if(foundValue != null){
        Assert.fail("Get succeeded for a non Existing key. Found a value: " + foundValue.toString() + "  for the key: " + key.toString());
      }
    } catch (VeniceStorageException e) {
      Assert.fail("VeniceStorageException was thrown: " + e.getMessage(), e);
    }
  }

  @Test
  public void testPutNullKey(){
    byte[] key = null;
    byte[] value = TestUtils.getRandomBytes(valueSize);
    int partitionId = TestUtils.getRandomIntInRange(numOfPartitions);
    try{
      doPut(partitionId, key, value);
    } catch(IllegalArgumentException e){
      // This is expected
      return;
    } catch (VeniceStorageException e) {
      Assert.fail("VeniceStorageException was thrown: " + e.getMessage(), e);
    }
    Assert.fail("Put succeeded for key: null and value: " + value.toString() +" unexpectedly");
  }
}
