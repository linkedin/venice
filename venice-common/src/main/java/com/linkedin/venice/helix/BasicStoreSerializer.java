package com.linkedin.venice.helix;

import java.io.Serializable;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.SerializationUtils;


/**
 * Created by yayan on 2/11/16.
 */
public class BasicStoreSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data)
        throws ZkMarshallingError {
        return SerializationUtils.serialize((Serializable) data);
    }

    @Override
    public Object deserialize(byte[] bytes)
        throws ZkMarshallingError {
        return SerializationUtils.deserialize(bytes);
    }
}
