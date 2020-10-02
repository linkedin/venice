package com.linkedin.venice.common;

import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import java.util.Arrays;
import java.util.Collections;


public class MetadataStoreUtils {
  public static StoreMetadataKey getStoreAttributesKey(String storeName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Collections.singletonList(storeName);
    key.metadataType = StoreMetadataType.STORE_ATTRIBUTES.getValue();
    return key;
  }

  public static StoreMetadataKey getTargetVersionStatesKey(String storeName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Collections.singletonList(storeName);
    key.metadataType = StoreMetadataType.TARGET_VERSION_STATES.getValue();
    return key;
  }

  public static StoreMetadataKey getCurrentStoreStatesKey(String storeName, String clusterName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Arrays.asList(storeName, clusterName);
    key.metadataType = StoreMetadataType.CURRENT_STORE_STATES.getValue();
    return key;
  }

  public static StoreMetadataKey getCurrentVersionStatesKey(String storeName, String clusterName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Arrays.asList(storeName, clusterName);
    key.metadataType = StoreMetadataType.CURRENT_VERSION_STATES.getValue();
    return key;
  }

  public static StoreMetadataKey getStoreKeySchemasKey(String storeName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Collections.singletonList(storeName);
    key.metadataType = StoreMetadataType.STORE_KEY_SCHEMAS.getValue();
    return key;
  }

  public static StoreMetadataKey getStoreValueSchemasKey(String storeName) {
    StoreMetadataKey key = new StoreMetadataKey();
    key.keyStrings = Collections.singletonList(storeName);
    key.metadataType = StoreMetadataType.STORE_VALUE_SCHEMAS.getValue();
    return key;
  }
}
