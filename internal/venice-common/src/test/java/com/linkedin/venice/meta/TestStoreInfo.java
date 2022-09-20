package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreInfo {
  private static final ObjectMapper OBJECT_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

  private static final String PRE_HYBRID_STORE_INFO_STRING =
      "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000}";
  private static final String STORE_INFO_WITH_EXTRA_FIELD_STRING =
      "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000,\"extrafield\":\"garbage\"}";

  @Test
  public void serializesAndDeserializesWithExtraFields() throws IOException {
    StoreInfo deserializedExtraFieldFasterXml =
        OBJECT_MAPPER.readValue(STORE_INFO_WITH_EXTRA_FIELD_STRING, StoreInfo.class);
    Assert.assertEquals(deserializedExtraFieldFasterXml.getName(), "store-1498761605131-33801192");
  }

  @Test
  public void deserializesWithMissingFields() throws IOException {
    StoreInfo deserializedMissingFieldFasterXml =
        OBJECT_MAPPER.readValue(PRE_HYBRID_STORE_INFO_STRING, StoreInfo.class);
    Assert.assertNull(deserializedMissingFieldFasterXml.getHybridStoreConfig());
  }
}
