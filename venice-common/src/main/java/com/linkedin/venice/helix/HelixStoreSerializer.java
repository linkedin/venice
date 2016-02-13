package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreSerializer;
import java.io.IOException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;


/**
 * Serializer to adapt to Zookeeper serializer. Use the Venice store serializer to do the actual serialize and
 * deserialize.
 */
public class HelixStoreSerializer implements ZkSerializer {
    private final StoreSerializer serializer;

    public HelixStoreSerializer(StoreSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(Object data)
        throws ZkMarshallingError {
        try {
            return serializer.serialize((Store) data);
        } catch (IOException e) {
            throw new ZkMarshallingError("Met error when serialize store.", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes)
        throws ZkMarshallingError {
        try {
            return serializer.deserialize(bytes);
        } catch (IOException e) {
            throw new ZkMarshallingError("Met error when deserialize store.", e);
        }
    }
}
