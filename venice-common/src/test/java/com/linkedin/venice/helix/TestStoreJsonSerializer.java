package com.linkedin.venice.helix;

import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;

import com.linkedin.venice.utils.Time;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test cases for StoreJsonSerializer.
 */
public class TestStoreJsonSerializer {
    @Test
    public void testSerializeAndDeserializeStore()
        throws IOException {
        Store store = TestUtils.createTestStore("s1", "owner", 1l);
        store.increaseVersion();
        HybridStoreConfig hybridStoreConfig = new HybridStoreConfig(1000, 1000);
        store.setHybridStoreConfig(hybridStoreConfig);
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store, "");
        Store newStore = serializer.deserialize(data, "");
        Assert.assertEquals(store,newStore);

        // Equality of the HybridStoreConfig should already be covered by the Store's equals(), but just in case it's not, we'll verify...
        Assert.assertEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());

        // Verify consistency between the two equals()...
        newStore.setHybridStoreConfig(new HybridStoreConfig(1000, 1));
        Assert.assertNotEquals(store, newStore);
        Assert.assertNotEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());
        newStore.setHybridStoreConfig(new HybridStoreConfig(1, 1000));
        Assert.assertNotEquals(store, newStore);
        Assert.assertNotEquals(store.getHybridStoreConfig(), newStore.getHybridStoreConfig());
    }

    @Test
    public void testSerializeAndDeserializeStoreMissingSomeFields()
        throws IOException {
        String jsonStr = "{\"name\":\"s1\"}";
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        Store store = serializer.deserialize(jsonStr.getBytes(), "");
        Assert.assertEquals(store.getName(), "s1");
        Assert.assertEquals(store.getOwner(), null);
        Assert.assertEquals(store.getCreatedTime(), 0);
        Assert.assertEquals(store.isHybrid(), false);
        Assert.assertEquals(store.getHybridStoreConfig(), null);
    }

    @Test
    public void testDeserializeStoreWithUnknownField()
        throws IOException {
        Store store = TestUtils.createTestStore("s1", "owner", 1l);
        store.increaseVersion();
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store, "");
        String jsonStr = new String(data);
        // Verify the backward compatibility, add a new feild in to Json string.
        jsonStr = jsonStr.substring(0, jsonStr.length()-1)+",\"unknown\":\"test\"}";
        Store newStore = serializer.deserialize(jsonStr.getBytes(), "");
        Assert.assertEquals(store,newStore);
    }

    @Test
    public void testDeserializeDisabledStore()
        throws IOException {
        Store store = TestUtils.createTestStore("s1", "owner", 1l);
        store.increaseVersion();
        store.setEnableReads(false);
        store.setEnableReads(false);
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store, "");
        String jsonStr = new String(data);
        Store newStore = serializer.deserialize(jsonStr.getBytes(), "");
        Assert.assertEquals(store, newStore);
    }
}
