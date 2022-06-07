package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreInfo {
  static ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

  static final String preHybridStoreInfoString = "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000}";
  static final String storeInfoWithExtraFieldString = "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000,\"extrafield\":\"garbage\"}";

  @Test
  public void serializesAndDeserializesWithExtraFields() throws IOException {
    StoreInfo deserializedExtraFieldFasterXml = objectMapper.readValue(storeInfoWithExtraFieldString, StoreInfo.class);
    Assert.assertEquals(deserializedExtraFieldFasterXml.getName(), "store-1498761605131-33801192");
  }

  @Test
  public void deserializesWithMissingFields() throws IOException {
    StoreInfo deserializedMissingFieldFasterXml = objectMapper.readValue(preHybridStoreInfoString, StoreInfo.class);
    Assert.assertNull(deserializedMissingFieldFasterXml.getHybridStoreConfig());
  }

  @Test
  public void testStoreInfoReturnsIncrementalPushPolicy() throws IOException {
    Store store = new ZKStore("testStore", "", 10, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
    store.setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);

    StoreInfo storeInfo = StoreInfo.fromStore(store);
    Assert.assertEquals(storeInfo.getIncrementalPushPolicy(), IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);

    // Serializing and deserializing to ensure data is present in the json
    String serializedStoreInfo = objectMapper.writeValueAsString(storeInfo);
    StoreInfo deserializedStoreInfo = objectMapper.readValue(serializedStoreInfo, StoreInfo.class);
    Assert.assertEquals(deserializedStoreInfo.getIncrementalPushPolicy(), IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
  }
}
