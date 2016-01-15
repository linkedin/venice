package com.linkedin.venice.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixSpectatorService;
import com.linkedin.venice.helix.PartitionLookup;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.HostPort;
import com.linkedin.venice.utils.Props;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Class which acts as the primary reader API
 */
public class VeniceReader<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceReader.class.getName());

  private Props props;
  private final String storeName;
  private final VeniceSerializer<K> keySerializer;
  private final VeniceSerializer<V> valueSerializer;

  public VeniceReader(Props props, String storeName, VeniceSerializer<K> keySerializer, VeniceSerializer<V> valueSerializer) {

    try {
      this.props = props;
      this.storeName = storeName;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (Exception e) {
      logger.error("Error while starting up configuration for VeniceReader.", e);
      throw new VeniceException("Error while starting up configuration for VeniceReader", e);
    }
  }

  /**
   * Execute a standard "get" on the key. Returns null if empty.
   * @param key - The key to look for in storage.
   * @return The result of the "Get" operation
   * */
  public V get(K key) {
    byte[] keyBytes = keySerializer.serialize(storeName, key);
    int partition = 0; // No partitioning currently happens on write?.
    PartitionLookup lookup = new PartitionLookup();
    HelixSpectatorService spectatorService = new HelixSpectatorService(
        props.getString("zookeeper.connection.string"),
        props.getString("cluster.name"),
        "client-spectator",
        lookup
        );

    List<HostPort> hosts;
    try {
      spectatorService.start();
      hosts = lookup.getHostPortForPartition(storeName, Integer.toString(partition));
      spectatorService.stop();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (hosts.size() < 1){
      throw new NullPointerException("No hosts available to serve partition: " + partition +
          ", store: " + storeName);
    }
    // Proper router will use a strategy other than "read from one"
    String host = hosts.get(0).getHostname();
    int port = Integer.parseInt(hosts.get(0).getPort());

    GetRequestObject request = new GetRequestObject(storeName.toCharArray(), partition, keyBytes);

    ReadClient client = new ReadClient();
    byte[] valueBytes = client.doRead(host, port, request);
    return valueSerializer.deserialize(storeName, valueBytes);
  }
}
