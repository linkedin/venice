package com.linkedin.venice.system.store;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_PARTITION_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_VERSION_NUMBER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * All the data types supported in meta system store.
 */
public enum MetaStoreDataType implements VeniceEnumValue {
  STORE_PROPERTIES(0, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_CLUSTER_NAME)),
  STORE_KEY_SCHEMAS(1, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_VALUE_SCHEMAS(2, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_REPLICA_STATUSES(
      3,
      Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_CLUSTER_NAME, KEY_STRING_VERSION_NUMBER, KEY_STRING_PARTITION_ID)
  ), STORE_CLUSTER_CONFIG(4, Collections.singletonList(KEY_STRING_STORE_NAME)),
  STORE_VALUE_SCHEMA(5, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_SCHEMA_ID)),
  VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION(6, Arrays.asList(KEY_STRING_STORE_NAME, KEY_STRING_VERSION_NUMBER)),
  HEARTBEAT(7, Collections.singletonList(KEY_STRING_STORE_NAME));

  private static final List<MetaStoreDataType> TYPES = EnumUtils.getEnumValuesList(MetaStoreDataType.class);

  private final int value;
  private final List<String> requiredKeys;

  MetaStoreDataType(int value, List<String> requiredKeys) {
    this.value = value;
    this.requiredKeys = requiredKeys;
  }

  @Override
  public int getValue() {
    return value;
  }

  public static MetaStoreDataType valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, MetaStoreDataType.class);
  }

  public StoreMetaKey getStoreMetaKey(Map<String, String> params) {
    List<CharSequence> keyStrings = new ArrayList<>();
    for (String requiredKey: requiredKeys) {
      String v = params.get(requiredKey);
      if (v == null) {
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
