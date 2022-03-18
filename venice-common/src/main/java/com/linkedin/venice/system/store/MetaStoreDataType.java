package com.linkedin.venice.system.store;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.linkedin.venice.system.store.MetaStoreWriter.*;


/**
 * All the data types supported in meta system store.
 */
public enum MetaStoreDataType {
  STORE_PROPERTIES(0, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_CLUSTER_NAME)),
  STORE_KEY_SCHEMAS(1, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_VALUE_SCHEMAS(2, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_REPLICA_STATUSES(3, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_CLUSTER_NAME, KEY_STRING_VERSION_NUMBER, KEY_STRING_PARTITION_ID)),
  STORE_CLUSTER_CONFIG(4, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_VALUE_SCHEMA(5, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_SCHEMA_ID)),
  VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION(6, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_VERSION_NUMBER));

  private final int value;
  private final List<String> requiredKeys;

  MetaStoreDataType(int value, List<String> requiredKeys) {
    this.value = value;
    this.requiredKeys = requiredKeys;
  }

  public int getValue() {
    return value;
  }

  public StoreMetaKey getStoreMetaKey(Map<String, String> params) {
    List<CharSequence> keyStrings = new ArrayList<>();
    for (String requiredKey : requiredKeys) {
      String v = params.get(requiredKey);
      if (null == v) {
        throw new VeniceException("keyString field: " + requiredKey + " is required for data type: " + name());
      }
      keyStrings.add(v);
    }
    StoreMetaKey storeMetaKey = new StoreMetaKey();
    storeMetaKey.keyStrings = keyStrings;
    storeMetaKey.metadataType = value;
    return storeMetaKey;
  }
}
