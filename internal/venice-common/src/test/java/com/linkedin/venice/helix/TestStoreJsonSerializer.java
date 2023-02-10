package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for StoreJsonSerializer.
 */
public class TestStoreJsonSerializer {
  @Test
  public void testSerializeAndDeserializeStore() throws IOException {
    Store store = TestUtils.createTestStore("s1", "owner", 1l);
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        1000,
        1000,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    store.setHybridStoreConfig(hybridStoreConfig);
    store.setReadQuotaInCU(100);

    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    viewConfigMap.put(
        "changeCapture",
        new ViewConfigImpl("com.linkedin.venice.views.ChangeCaptureView", Collections.emptyMap()));
    store.setViewConfigs(viewConfigMap);

    StoreJSONSerializer serializer = new StoreJSONSerializer();
    byte[] data = serializer.serialize(store, "");
    Store newStore = serializer.deserialize(data, "");
    Assert.assertEquals(store, newStore);

    // Equality of the HybridStoreConfig should already be covered by the Store's equals(), but just in case it's not,
    // we'll verify...
    Assert.assertEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());

    // Verify consistency between the two equals()...
    newStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            1000,
            1,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    Assert.assertNotEquals(store, newStore);
    Assert.assertNotEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());
    newStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            1,
            1000,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    Assert.assertNotEquals(store, newStore);
    Assert.assertNotEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());
  }

  @Test
  public void testSerializeAndDeserializeStoreMissingSomeFields() throws IOException {
    String jsonStr = "{\"name\":\"s1\"}";
    StoreJSONSerializer serializer = new StoreJSONSerializer();
    Store store = serializer.deserialize(jsonStr.getBytes(), "");
    Assert.assertEquals(store.getName(), "s1");
    Assert.assertNull(store.getOwner());
    Assert.assertEquals(store.getCreatedTime(), 0);
    Assert.assertFalse(store.isHybrid());
    Assert.assertNull(store.getHybridStoreConfig());
    Assert.assertNotNull(store.getPartitionerConfig());
    Assert.assertEquals(store.getPartitionerConfig().getAmplificationFactor(), 1);
    Assert.assertEquals(store.getPartitionerConfig().getPartitionerClass(), DefaultVenicePartitioner.class.getName());
    Assert.assertEquals(store.getPartitionerConfig().getPartitionerParams(), new HashMap<>());
    Assert
        .assertTrue(store.isEnableReads(), "By default, allow store to be read while missing enableReads field in ZK");
    Assert.assertTrue(
        store.isEnableWrites(),
        "By default, allow store to be written while missing enableWrites field in ZK");
  }

  @Test
  public void testDeserializeStoreWithUnknownField() throws IOException {
    Store store = TestUtils.createTestStore("s1", "owner", 1l);
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
    store.setReadQuotaInCU(100);
    StoreJSONSerializer serializer = new StoreJSONSerializer();
    byte[] data = serializer.serialize(store, "");
    String jsonStr = new String(data);
    // Verify the backward compatibility, add a new feild in to Json string.
    jsonStr = jsonStr.substring(0, jsonStr.length() - 1) + ",\"unknown\":\"test\"}";
    Store newStore = serializer.deserialize(jsonStr.getBytes(), "");
    Assert.assertEquals(store, newStore);
  }

  @Test
  public void testDeserializeDisabledStore() throws IOException {
    Store store = TestUtils.createTestStore("s1", "owner", 1l);
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
    store.setEnableReads(false);
    store.setEnableWrites(false);
    store.setReadQuotaInCU(100);
    StoreJSONSerializer serializer = new StoreJSONSerializer();
    byte[] data = serializer.serialize(store, "");
    String jsonStr = new String(data);
    Store newStore = serializer.deserialize(jsonStr.getBytes(), "");
    Assert.assertEquals(store, newStore);
  }

  @Test
  public void testSerializationWithSystemStores() throws IOException {
    Store store = TestUtils.createTestStore("s1", "owner", 1l);
    store.addVersion(new VersionImpl(store.getName(), 1, "test_push_id"));

    SystemStoreAttributes systemStoreAttributes = new SystemStoreAttributesImpl();
    systemStoreAttributes.setCurrentVersion(1);
    systemStoreAttributes.setLargestUsedVersionNumber(1);
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.META_STORE;
    String systemStoreName = systemStoreType.getSystemStoreName(store.getName());
    Version systemStoreVersion = new VersionImpl(systemStoreName, 1, "test_push_id");
    systemStoreAttributes.setVersions(Arrays.asList(systemStoreVersion));
    Map<String, SystemStoreAttributes> systemStores = new HashMap<>();
    systemStores.put(systemStoreType.getPrefix(), systemStoreAttributes);
    store.setSystemStores(systemStores);
    StoreJSONSerializer serializer = new StoreJSONSerializer();
    byte[] serializedBytes = serializer.serialize(store, "test");
    Store deserializedStore = serializer.deserialize(serializedBytes, "test");
    Assert.assertTrue(deserializedStore.getVersion(1).isPresent(), "Version 1 should exist");
    Map<String, SystemStoreAttributes> deserializedStoreSystemStores = deserializedStore.getSystemStores();
    Assert.assertTrue(
        deserializedStoreSystemStores.containsKey(systemStoreType.getPrefix()),
        "System store attributes for type: " + systemStoreType.getPrefix() + " must exist");
    SystemStoreAttributes systemStoreAttributesForTestType =
        deserializedStoreSystemStores.get(systemStoreType.getPrefix());
    Assert.assertEquals(systemStoreAttributesForTestType.getCurrentVersion(), 1);
    Assert.assertEquals(systemStoreAttributesForTestType.getLargestUsedVersionNumber(), 1);
    Assert.assertEquals(systemStoreAttributesForTestType.getVersions().size(), 1);
  }
}
