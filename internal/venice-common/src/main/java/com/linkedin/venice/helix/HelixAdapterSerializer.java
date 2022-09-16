package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.PathResourceRegistry;
import com.linkedin.venice.utils.TrieBasedPathResourceRegistry;
import java.io.IOException;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;


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

  public void unregisterSerializer(String path) {
    pathResourceRegistry.unregister(path);
  }

  @Override
  public byte[] serialize(Object data, String path) throws ZkMarshallingError {
    try {
      VeniceSerializer serializer = getSerializer(path);
      if (serializer == null) {
        throw new VeniceException("Failed to get serializer for path: " + path);
      }
      return serializer.serialize(data, path);
    } catch (IOException e) {
      throw new ZkMarshallingError("Met error when serialize object for path: " + path, e);
    }
  }

  @Override
  public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError {
    try {
      VeniceSerializer serializer = getSerializer(path);
      if (serializer == null) {
        throw new VeniceException("Failed to get serializer for path: " + path);
      }
      return serializer.deserialize(bytes, path);
    } catch (IOException e) {
      throw new ZkMarshallingError("Met error when deserialize object for path: " + path, e);
    }
  }

  private VeniceSerializer getSerializer(String path) {
    return pathResourceRegistry.find(path);
  }
}
