package com.linkedin.venice.meta;

import java.io.IOException;


/**
 * Interface defines how to serialize and deserilize store.
 */
public interface StoreSerializer {
    public byte[] serialize(Store store)
        throws IOException;

    public Store deserialize(byte[] bytes)
        throws IOException;
}
