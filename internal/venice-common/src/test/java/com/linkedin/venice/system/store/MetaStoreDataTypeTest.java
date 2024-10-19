package com.linkedin.venice.system.store;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class MetaStoreDataTypeTest extends VeniceEnumValueTest<MetaStoreDataType> {
  public MetaStoreDataTypeTest() {
    super(MetaStoreDataType.class);
  }

  @Override
  protected Map<Integer, MetaStoreDataType> expectedMapping() {
    return CollectionUtil.<Integer, MetaStoreDataType>mapBuilder()
        .put(0, MetaStoreDataType.STORE_PROPERTIES)
        .put(1, MetaStoreDataType.STORE_KEY_SCHEMAS)
        .put(2, MetaStoreDataType.STORE_VALUE_SCHEMAS)
        .put(3, MetaStoreDataType.STORE_REPLICA_STATUSES)
        .put(4, MetaStoreDataType.STORE_CLUSTER_CONFIG)
        .put(5, MetaStoreDataType.STORE_VALUE_SCHEMA)
        .put(6, MetaStoreDataType.VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION)
        .put(7, MetaStoreDataType.HEARTBEAT)
        .build();
  }
}
