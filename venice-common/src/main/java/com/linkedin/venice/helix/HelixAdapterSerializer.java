package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;

import com.linkedin.venice.utils.PathResourceRegistry;
import com.linkedin.venice.utils.TrieBasedPathResourceRegistry;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.apache.helix.manager.zk.PathBasedZkSerializer;


/**
 * Serializer to adapt venice serializer to PathBasedZkSerializer. Find the venice serializer by given zk path and use
 * it to do the actual serialize and deserialize.
 * <p>
 * This class let venice can re-use on ZkClient to read/write different types of Venice objects.
 */
public class HelixAdapterSerializer implements PathBasedZkSerializer {

  private PathResourceRegistry<VeniceSerializer> pathResourceRegistry;

  public HelixAdapterSerializer() {
    pathResourceRegistry = new TrieBasedPathResourceRegistry<>();
  }

  public void registerSerializer(String path, VeniceSerializer serializer) {
    pathResourceRegistry.register(path, serializer);
  }

  public void unregisterSeralizer(String path) {
    pathResourceRegistry.unregister(path);
  }

  @Override
  public byte[] serialize(Object data, String path)
      throws ZkMarshallingError {
    try {
      return getSerializer(path).serialize(data, path);
    } catch (IOException e) {
      throw new ZkMarshallingError("Met error when serialize store.", e);
    }
  }

  @Override
  public Object deserialize(byte[] bytes, String path)
      throws ZkMarshallingError {
    try {
      return getSerializer(path).deserialize(bytes, path);
    } catch (IOException e) {
      throw new ZkMarshallingError("Met error when deserialize store.", e);
    }
  }

  private VeniceSerializer getSerializer(String path){
    return pathResourceRegistry.find(path);
  }
}
