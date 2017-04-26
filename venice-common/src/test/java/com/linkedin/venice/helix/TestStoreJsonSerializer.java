package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        List<String> principles = new ArrayList<>();
        principles.add("test");
        store.setPrinciples(principles);
        store.increaseVersion();
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store, "");
        Store newStore = serializer.deserialize(data, "");
        Assert.assertEquals(store,newStore);
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
