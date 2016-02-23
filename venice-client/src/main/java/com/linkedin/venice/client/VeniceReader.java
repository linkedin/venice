package com.linkedin.venice.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixSpectatorService;
import com.linkedin.venice.kafka.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.kafka.partitioner.VenicePartitioner;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.serialization.VeniceSerializer;
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
  private HelixSpectatorService spectatorService;

  //TODO: configurable partitioner
  private final VenicePartitioner partitioner = new DefaultVenicePartitioner();

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

  public void init(){
    spectatorService = new HelixSpectatorService(
        props.getString("zookeeper.connection.string"),
        props.getString("cluster.name"),
        "client-spectator" //need some unique name for each client/spectator?
    );
    try{
      spectatorService.start();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  /**
   * Execute a standard "get" on the key. Returns null if empty.
   * @param key - The key to look for in storage.
   * @return The result of the "Get" operation
   * */
  public V get(K key) {
    byte[] keyBytes = keySerializer.serialize(storeName, key);

    List<Instance> instances;
    int partition;
    int numberOfPartitions = spectatorService.getRoutingDataRepository().getNumberOfPartitions(storeName);
    KafkaKey kafkaKey = new KafkaKey(null, keyBytes);
    partition = partitioner.getPartitionId(kafkaKey, numberOfPartitions);
    instances=spectatorService.getRoutingDataRepository().getInstances(storeName, partition);
    if (instances.size() < 1){
      throw new NullPointerException("No hosts available to serve partition: " + partition +
          ", store: " + storeName);
    }
    // Proper router will use a strategy other than "read from one"
    String host = instances.get(0).getHost();
    int port = instances.get(0).getHttpPort();

    GetRequestObject request = new GetRequestObject(storeName.toCharArray(), partition, keyBytes);

    ReadClient client = new ReadClient();
    byte[] valueBytes = client.doRead(host, port, request);
    return valueSerializer.deserialize(storeName, valueBytes);
  }
}
