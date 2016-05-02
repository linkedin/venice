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
        StoreJSONSerializer serializer = new StoreJSONSerializer();
        byte[] data = serializer.serialize(store);

        System.out.println(new String(data));

        Store newStore = serializer.deserialize(data);
        Assert.assertEquals(store,newStore);
    }
}
