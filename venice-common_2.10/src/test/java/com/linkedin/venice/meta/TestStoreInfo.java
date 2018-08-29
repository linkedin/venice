package com.linkedin.venice.meta;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreInfo {
  static com.fasterxml.jackson.databind.ObjectMapper fasterXmlMapper = new com.fasterxml.jackson.databind.ObjectMapper();
  static org.codehaus.jackson.map.ObjectMapper codehouseMapper = new org.codehaus.jackson.map.ObjectMapper();

  static final String preHybridStoreInfoString = "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000}";
  static final String storeInfoWithExtraFieldString = "{\"name\":\"store-1498761605131-33801192\",\"owner\":\"owner-1498761605134-176696364\",\"currentVersion\":2,\"reservedVersion\":0,\"partitionCount\":5,\"enableStoreWrites\":true,\"enableStoreReads\":true,\"versions\":[{\"storeName\":\"store-1498761605131-33801192\",\"number\":2,\"createdTime\":1498761605133,\"pushJobId\":\"pushId-1498761605133-938151108\",\"status\":\"STARTED\"}],\"storageQuotaInByte\":-2147483648,\"readQuotaInCU\":3000,\"extrafield\":\"garbage\"}";

  @Test
  public void serializesAndDeserializesWithExtraFields() throws IOException {
    StoreInfo deserializedExtraFieldFasterXml = fasterXmlMapper.readValue(storeInfoWithExtraFieldString, StoreInfo.class);
    Assert.assertEquals(deserializedExtraFieldFasterXml.getName(), "store-1498761605131-33801192");

    StoreInfo deserializedExtraFieldCodehouse = codehouseMapper.readValue(storeInfoWithExtraFieldString, StoreInfo.class);
    Assert.assertEquals(deserializedExtraFieldCodehouse.getName(), "store-1498761605131-33801192");
  }

  @Test
  public void deserializesWithMissingFields() throws IOException {
    StoreInfo deserializedMissingFieldFasterXml = fasterXmlMapper.readValue(preHybridStoreInfoString, StoreInfo.class);
    Assert.assertNull(deserializedMissingFieldFasterXml.getHybridStoreConfig());

    StoreInfo deserializedMissingFieldCodehouse = codehouseMapper.readValue(preHybridStoreInfoString, StoreInfo.class);
    Assert.assertNull(deserializedMissingFieldCodehouse.getHybridStoreConfig());
  }

}
