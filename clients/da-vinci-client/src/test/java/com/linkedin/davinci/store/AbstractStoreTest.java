package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.RandomGenUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.testng.Assert;


/**
 * Test the operations of the Store Interface
 * 1. put, get, delete and overwriting put operations
 * 2. Test non existent Keys
 * 3. Test null keys
 * 4.
 */
public abstract class AbstractStoreTest {
  protected AbstractStorageEngine<?> testStore;
  protected int numOfPartitions = 1;
  int keySize = 50;
  int valueSize = 500;
  int uniqueKeyOrValueSize = 350;

  // creates instance for testStore
  public abstract void createStoreForTest() throws Exception;

  protected byte[] doGet(int partitionId, byte[] key) throws VeniceException {
    byte[] result;
    result = testStore.get(partitionId, key);
    return result;
  }

  protected void doGetByKeyPrefix(int partitionId, byte[] partialKey, BytesStreamingCallback callback) {
    testStore.getByKeyPrefix(partitionId, partialKey, callback);
  }

  protected void doPut(int partitionId, byte[] key, byte[] value) throws VeniceException {
    testStore.put(partitionId, key, value);
  }

  protected void doDelete(int partitionId, byte[] key) throws VeniceException {
    testStore.delete(partitionId, key);
  }

  public void testGetAndPut() {
    byte[] key = RandomGenUtils.getRandomBytes(keySize);
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);
    byte[] foundValue;
    try {
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(
          value,
          foundValue,
          "The actual value: " + Hex.encodeHexString(value) + " and expected value: " + Hex.encodeHexString(foundValue)
              + " do not match!");
    } catch (VeniceException e) {
      Assert.fail("Exception was thrown: " + e.getMessage(), e);
    }
  }

  public void testGetByKeyPrefixManyKeys() {
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);
    String keyPrefixString = "key_";
    String valuePrefixString = "value_";
    String ABCString = "ABC_";

    try {
      int numKeysInGroup = 100;

      // Initialize list of ByteArray values for value_1, ..., value_100
      List<ByteArray> values = initializeByteArrayList(numKeysInGroup, valuePrefixString);

      // Initialize list of ByteArray keys for key_1, ..., key_100
      List<ByteArray> keys_key = initializeByteArrayList(numKeysInGroup, keyPrefixString);

      // Initialize list of ByteArray keys for key_ABC_1, ..., key_ABC_100
      List<ByteArray> keys_keyABC = initializeByteArrayList(numKeysInGroup, keyPrefixString + ABCString);

      // Initialize list of ByteArray keys for ABC_1, ..., ABC_100
      List<ByteArray> keys_ABC = initializeByteArrayList(numKeysInGroup, ABCString);

      // create list of key value pairs to be added to database
      List<Pair<byte[], byte[]>> keyValuePairsToPut = new ArrayList<>();
      for (int i = 0; i < numKeysInGroup; i++) {
        keyValuePairsToPut.add(new Pair(keys_key.get(i).get(), values.get(i).get()));
        keyValuePairsToPut.add(new Pair(keys_keyABC.get(i).get(), values.get(i).get()));
        keyValuePairsToPut.add(new Pair(keys_ABC.get(i).get(), values.get(i).get()));
      }

      // shuffle keyValuePairsToPut to ensure key value pairs get put into db in random order
      Collections.shuffle(keyValuePairsToPut);

      for (Pair<byte[], byte[]> kvp: keyValuePairsToPut) {
        doPut(partitionId, kvp.getFirst(), kvp.getSecond());
      }

      final Map<ByteArray, ByteArray> getByPartialKeyResultMap = new HashMap<>();
      final boolean[] isCompleted = { false };
      BytesStreamingCallback callback = new BytesStreamingCallback() {
        @Override
        public void onRecordReceived(byte[] key, byte[] value) {
          getByPartialKeyResultMap.put(new ByteArray(key), new ByteArray(value));
        }

        @Override
        public void onCompletion() {
          isCompleted[0] = true;
        }
      };

      // Test getting everything whose key has the prefix "key_"
      doGetByKeyPrefix(partitionId, keyPrefixString.getBytes(), callback);
      Assert.assertNotNull(getByPartialKeyResultMap);
      Assert.assertEquals(getByPartialKeyResultMap.size(), numKeysInGroup * 2);

      // check that (key_1, value_1), ..., (key_100, value_100) and
      // (key_ABC_1, value_1), ..., (key_ABC_100, value_100) are present
      for (int i = 0; i < numKeysInGroup; i++) {
        Assert.assertEquals(getByPartialKeyResultMap.get(keys_key.get(i)), values.get(i));
        Assert.assertEquals(getByPartialKeyResultMap.get(keys_keyABC.get(i)), values.get(i));
      }

      getByPartialKeyResultMap.clear();

      // Test getting everything whose key has the prefix "key_ABC_"
      doGetByKeyPrefix(partitionId, (keyPrefixString + ABCString).getBytes(), callback);
      Assert.assertNotNull(getByPartialKeyResultMap);
      Assert.assertEquals(getByPartialKeyResultMap.size(), numKeysInGroup);

      // check that (key_ABC_1, value_1) ... (key_ABC_100, value_100) are present
      for (int i = 0; i < numKeysInGroup; i++) {
        Assert.assertEquals(getByPartialKeyResultMap.get(keys_keyABC.get(i)), values.get(i));
      }

      getByPartialKeyResultMap.clear();

      // Test getting everything whose key has the prefix "ABC_"
      doGetByKeyPrefix(partitionId, ABCString.getBytes(), callback);
      Assert.assertNotNull(getByPartialKeyResultMap);
      Assert.assertEquals(getByPartialKeyResultMap.size(), numKeysInGroup);

      // check that (ABC_1, value_1) ... (ABC_100, value_100) are present
      for (int i = 0; i < numKeysInGroup; i++) {
        Assert.assertEquals(getByPartialKeyResultMap.get(keys_ABC.get(i)), values.get(i));
      }

    } catch (VeniceException e) {
      Assert.fail("Exception was thrown: " + e.getMessage(), e);
    }
  }

  private List<ByteArray> initializeByteArrayList(int listSize, String prefix) {
    List<ByteArray> byteArrayList = new ArrayList<>();
    for (int i = 1; i <= listSize; i++) {
      byteArrayList.add(new ByteArray((prefix + i).getBytes()));
    }
    return byteArrayList;
  }

  public void testGetByKeyPrefixMaxSignedByte() {
    byte[] prefix = { 65, 127 };
    // keys which should be found
    byte[] key1 = { 65, 127 };
    byte[] key2 = { 65, 127, 0 };
    byte[] key3 = { 65, 127, -128 };

    // keys which should NOT be found
    byte[] key4 = { 65, -128, 0 };
    byte[] key5 = { 65, 0 };
    byte[] key6 = { 65, -100 };

    List<byte[]> keysToBeFound = new ArrayList<>(Arrays.asList(key1, key2, key3));
    List<byte[]> keysNotToBeFound = new ArrayList<>(Arrays.asList(key4, key5, key6));

    testGetByKeyPrefix(prefix, keysToBeFound, keysNotToBeFound);
  }

  public void testGetByKeyPrefixMaxUnsignedByte() {
    byte[] prefix = { 65, -1 };
    // keys which should be found
    byte[] key1 = { 65, -1 };
    byte[] key2 = { 65, -1, 0 };
    byte[] key3 = { 65, -1, -128 };

    // keys which should NOT be found
    byte[] key4 = { 65, -2, 0 };
    byte[] key5 = { 65, 0 };
    byte[] key6 = { 65, 0, -3 };
    byte[] key7 = { 66, 127 };

    List<byte[]> keysToBeFound = new ArrayList<>(Arrays.asList(key1, key2, key3));
    List<byte[]> keysNotToBeFound = new ArrayList<>(Arrays.asList(key4, key5, key6, key7));

    testGetByKeyPrefix(prefix, keysToBeFound, keysNotToBeFound);
  }

  public void testGetByKeyPrefixByteOverflow() {
    byte[] maxBytesPrefix = { -1, -1 };
    // keys which should be found
    byte[] key1 = { -1, -1 };
    byte[] key2 = { -1, -1, 0 };
    byte[] key3 = { -1, -1, -128 };

    // keys which should NOT be found
    byte[] key4 = { -1, -2, 0 };
    byte[] key5 = { 0, 0 };
    byte[] key6 = { -1, 0, -3 };
    byte[] key7 = { 127, -1, -2, -1, 127 };

    List<byte[]> keysToBeFound = new ArrayList<>(Arrays.asList(key1, key2, key3));
    List<byte[]> keysNotToBeFound = new ArrayList<>(Arrays.asList(key4, key5, key6, key7));
    testGetByKeyPrefix(maxBytesPrefix, keysToBeFound, keysNotToBeFound);
  }

  private void testGetByKeyPrefix(byte[] prefix, List<byte[]> keysToBeFound, List<byte[]> keysNotToBeFound) {
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);

    final Map<ByteArray, ByteArray> getByPartialKeyResultMap = new HashMap<>();
    final boolean[] isCompleted = { false };
    try {
      for (byte[] key: keysToBeFound) {
        doPut(partitionId, key, value);
      }

      for (byte[] key: keysNotToBeFound) {
        doPut(partitionId, key, value);
      }

      BytesStreamingCallback callback = new BytesStreamingCallback() {
        @Override
        public void onRecordReceived(byte[] key, byte[] value) {
          getByPartialKeyResultMap.put(new ByteArray(key), new ByteArray(value));
        }

        @Override
        public void onCompletion() {
          isCompleted[0] = true;
        }
      };

      doGetByKeyPrefix(partitionId, prefix, callback);

      Assert.assertEquals(getByPartialKeyResultMap.size(), keysToBeFound.size());
      Assert.assertTrue(isCompleted[0]);

      ByteArray valueByteArray = new ByteArray(value);
      ByteArray keyByteArray;

      for (byte[] key: keysToBeFound) {
        keyByteArray = new ByteArray(key);
        Assert.assertEquals(getByPartialKeyResultMap.get(keyByteArray), valueByteArray);
      }

      for (byte[] key: keysNotToBeFound) {
        keyByteArray = new ByteArray(key);
        Assert.assertNull(getByPartialKeyResultMap.get(keyByteArray));
      }
    } catch (VeniceException e) {
      Assert.fail("Exception was thrown: " + e.getMessage(), e);
    }
  }

  public void testDelete() {
    byte[] key = RandomGenUtils.getRandomBytes(keySize);
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);
    byte[] foundValue;
    try {
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(
          value,
          foundValue,
          "The actual value: " + Hex.encodeHexString(value) + " and expected value: " + Hex.encodeHexString(foundValue)
              + " do not match!");
      doDelete(partitionId, key);
      try {
        foundValue = doGet(partitionId, key);
        if (foundValue != null) {
          Assert.fail(
              "Delete failed. found a value: " + Hex.encodeHexString(foundValue) + "  for the key: "
                  + Hex.encodeHexString(key) + " after deletion. ");
        }
      } catch (PersistenceFailureException e) {
        // This is expected.
      }
    } catch (Exception e) {
      Assert.fail("Exception was thrown: " + e.getMessage(), e);
    }
  }

  public void testUpdate() {
    byte[] key = RandomGenUtils.getRandomBytes(keySize);
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);
    byte[] updatedValue = RandomGenUtils.getRandomBytes(uniqueKeyOrValueSize);
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);
    byte[] foundValue;
    try {
      doPut(partitionId, key, value);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(
          value,
          foundValue,
          "The actual value: " + Hex.encodeHexString(value) + " and expected value: " + Hex.encodeHexString(foundValue)
              + " do not match!");
      doPut(partitionId, key, updatedValue);
      foundValue = doGet(partitionId, key);
      Assert.assertEquals(
          updatedValue,
          foundValue,
          "The updated value: " + Hex.encodeHexString(updatedValue) + " and expected value: "
              + Hex.encodeHexString(foundValue) + " do not match!");
    } catch (VeniceException e) {
      Assert.fail("Exception was thrown: " + e.getMessage(), e);
    }
  }

  public void testGetInvalidKeys() {
    byte[] key = RandomGenUtils.getRandomBytes(uniqueKeyOrValueSize);
    int partitionId = RandomGenUtils.getRandomIntWithin(numOfPartitions);
    byte[] foundValue = null;
    try {
      foundValue = doGet(partitionId, key);
      if (foundValue != null) {
        Assert.fail(
            "Get succeeded for a non Existing key. Found a value: " + Hex.encodeHexString(foundValue)
                + "  for the key: " + Hex.encodeHexString(key));
      }
    } catch (Exception e) {
      // This is expected.
      Assert.assertEquals(e.getClass(), VeniceException.class);
    }
  }
}
