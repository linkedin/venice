package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.apache.helix.manager.zk.PathBasedZkSerializer;


/**
 * Serializer to adapt venice serializer to PathBasedZkSerializer. Find the venice serializer by given zk path and use
 * it to do the actual serialize and deserialize.
 * <p>
 * This class let venice can re-use on ZkClient to read/write different types of Venice objects.
 */
public class HelixAdapterSerializer implements PathBasedZkSerializer {

  private Map<String, VeniceSerializer> pathToSerializers;

  public HelixAdapterSerializer() {
    pathToSerializers = new ConcurrentHashMap<>();
  }

  public void registerSerializer(String path, VeniceSerializer serializer) {
    pathToSerializers.put(path, serializer);
  }

  public void unregisterSeralizer(String path) {
    pathToSerializers.remove(path);
  }

  @Override
  public byte[] serialize(Object data, String path)
      throws ZkMarshallingError {
    try {
      //When registering serializer, there are two types of path:
      //1. /cluster/xxxx  : this is used for some parent objects like Job, path given here will be /cluster/Jobs/$jobId.
      //   So we can simple drop the last part /$jobId to find the path used when registering.
      //2. /cluster/xxxx/ : this is used for some children object. For example Task is. path given here will be
      //   /cluster/Jobs/$jobId/$partitionId, even after ignore the last part /$paritionId, the path should be also dynamic
      //   because $jobId are different. So we use /cluster/Jobs/ as the registering path of Task. When find serializer, drop
      //   the last part ast first, if we still can not find the serializer, drop the second last part but kee the last '/'
      //   to find again.
      String registeredPath = path.substring(0, path.lastIndexOf('/'));
      VeniceSerializer serializer = pathToSerializers.get(registeredPath);
      if (serializer == null) {
        serializer = pathToSerializers.get(registeredPath.substring(0, registeredPath.lastIndexOf('/') + 1));
      }
      return serializer.serialize(data);
    } catch (IOException | NullPointerException | IndexOutOfBoundsException e) {
      throw new ZkMarshallingError("Met error when serialize store.", e);
    }
  }

  @Override
  public Object deserialize(byte[] bytes, String path)
      throws ZkMarshallingError {
    try {
      String registeredPath = path.substring(0, path.lastIndexOf('/'));
      VeniceSerializer serializer = pathToSerializers.get(registeredPath);
      if (serializer == null) {
        serializer = pathToSerializers.get(registeredPath.substring(0, registeredPath.lastIndexOf('/') + 1));
      }
      return serializer.deserialize(bytes);
    } catch (IOException | NullPointerException | IndexOutOfBoundsException e) {
      throw new ZkMarshallingError("Met error when deserialize store.", e);
    }
  }
}
