package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
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
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store);
        Store newStore = serializer.deserialize(data);
        Assert.assertEquals(store,newStore);
    }

    @Test
    public void testSerializeAndDeserializeStoreMissingSomeFields()
        throws IOException {
        String jsonStr = "{\"name\":\"s1\"}";
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        Store store = serializer.deserialize(jsonStr.getBytes());
        Assert.assertEquals(store.getName(), "s1");
        Assert.assertEquals(store.getOwner(), null);
        Assert.assertEquals(store.getCreatedTime(), 0);
    }
}
