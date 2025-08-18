package com.linkedin.venice.meta;

import static com.linkedin.venice.utils.ConfigCommonUtils.ActivationState;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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
    assertEquals(deserializedExtraFieldFasterXml.getName(), "store-1498761605131-33801192");
  }

  @Test
  public void deserializesWithMissingFields() throws IOException {
    StoreInfo deserializedMissingFieldFasterXml =
        OBJECT_MAPPER.readValue(PRE_HYBRID_STORE_INFO_STRING, StoreInfo.class);
    assertNull(deserializedMissingFieldFasterXml.getHybridStoreConfig());
  }

  @Test
  public void nullStore() {
    assertNull(StoreInfo.fromStore(null)); // should not throw a NPE!
  }

  @Test
  public void testSetAndGetIsStoreDead() {
    StoreInfo storeInfo = new StoreInfo();
    assertFalse(storeInfo.getIsStoreDead(), "Default should be false");

    storeInfo.setIsStoreDead(true);
    assertTrue(storeInfo.getIsStoreDead(), "Should reflect store is dead");

    storeInfo.setIsStoreDead(false);
    assertFalse(storeInfo.getIsStoreDead(), "Should allow unsetting dead flag");
  }

  @Test
  public void testSetAndGetStoreDeadStatusReasons() {
    StoreInfo storeInfo = new StoreInfo();
    assertNotNull(storeInfo.getStoreDeadStatusReasons(), "Default should be empty list");
    assertTrue(storeInfo.getStoreDeadStatusReasons().isEmpty(), "Default should be empty");

    storeInfo.setStoreDeadStatusReasons(Arrays.asList("No versions", "Metadata corrupted"));
    assertEquals(2, storeInfo.getStoreDeadStatusReasons().size());
    assertEquals("No versions", storeInfo.getStoreDeadStatusReasons().get(0));

    storeInfo.setStoreDeadStatusReasons(null);
    assertNotNull(storeInfo.getStoreDeadStatusReasons(), "Null input should be handled");
    assertTrue(storeInfo.getStoreDeadStatusReasons().isEmpty(), "Null input should be treated as empty list");

    storeInfo.setStoreDeadStatusReasons(Collections.singletonList("Store deleted"));
    assertEquals(1, storeInfo.getStoreDeadStatusReasons().size());
    assertEquals("Store deleted", storeInfo.getStoreDeadStatusReasons().get(0));
  }

  @Test
  public void testSetAndGetStoreLifecycleHooks() {
    StoreInfo storeInfo = new StoreInfo();
    assertNotNull(storeInfo.getStoreLifecycleHooks(), "Default should be empty list");
    assertTrue(storeInfo.getStoreLifecycleHooks().isEmpty(), "Default should be empty");

    LifecycleHooksRecordImpl lifecycleHooksRecord = new LifecycleHooksRecordImpl();
    lifecycleHooksRecord.setStoreLifecycleHooksClassName("test");
    lifecycleHooksRecord.setStoreLifecycleHooksParams(Collections.emptyMap());
    storeInfo.setStoreLifecycleHooks(Arrays.asList(lifecycleHooksRecord));
    assertEquals(1, storeInfo.getStoreLifecycleHooks().size());
  }

  @Test
  public void testBlobTransferStoreLevelConfigs() {
    StoreInfo storeInfo = new StoreInfo();
    // check default value
    assertNotNull(storeInfo.getBlobTransferInServerEnabled());
    assertEquals(ActivationState.NOT_SPECIFIED.name(), storeInfo.getBlobTransferInServerEnabled());
    assertFalse(storeInfo.isBlobTransferEnabled());
    // setting value
    storeInfo.setBlobTransferInServerEnabled(ActivationState.ENABLED.name());
    storeInfo.setBlobTransferEnabled(true);
    // check updated value
    assertNotNull(storeInfo.getBlobTransferInServerEnabled());
    assertEquals(ActivationState.ENABLED.name(), storeInfo.getBlobTransferInServerEnabled());
    assertTrue(storeInfo.isBlobTransferEnabled());
  }
}
