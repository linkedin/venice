package com.linkedin.venice.client;

import static com.linkedin.venice.ConfigKeys.*;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixSpectatorService;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;

/**
 * Class which acts as the primary reader API
 */
public class VeniceReader<K, V> {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceReader.class);

  private VeniceProperties props;
  private final String resourceName;
  private final VeniceSerializer<K> keySerializer;
  private final VeniceSerializer<V> valueSerializer;
  private boolean initialized;
  private HelixSpectatorService spectatorService;

  //TODO: configurable partitioner
  private final VenicePartitioner partitioner = new DefaultVenicePartitioner();

  /**
   * @param resourceName the full Helix resource name (i.e.: store name + data version). The logic for choosing
   *                     which data version to query is handled higher up the stack.
   */
  public VeniceReader(VeniceProperties props, String resourceName, VeniceSerializer<K> keySerializer, VeniceSerializer<V> valueSerializer) {
    this.props = props;
    this.resourceName = resourceName;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.initialized = false;
  }

  public synchronized void init(){
    if (!initialized) {
      spectatorService = new HelixSpectatorService(
          props.getString(ZOOKEEPER_ADDRESS),
          props.getString(CLUSTER_NAME),
          "client-spectator" //need some unique name for each client/spectator?
      );
      try{
        spectatorService.start();
        initialized = true;
      } catch (Exception e) {
        throw new VeniceException("VeniceReader initialization failed!", e);
      }
    }
  }

  private void checkInit() {
    if (!initialized) {
      throw new VeniceException("This VeniceReader instance has not been initialized!");
    }
  }
  /**
   * Execute a standard "get" on the key. Returns null if empty.
   * @param key - The key to look for in storage.
   * @return The result of the "Get" operation
   * */
  public V get(K key) {
    checkInit();
    byte[] keyBytes = keySerializer.serialize(resourceName, key);

    List<Instance> instances;
    int partition;
    int numberOfPartitions = spectatorService.getRoutingDataRepository().getNumberOfPartitions(resourceName);
    partition = partitioner.getPartitionId(keyBytes, numberOfPartitions);
    instances=spectatorService.getRoutingDataRepository().getReadyToServeInstances(resourceName, partition);
    if (instances.size() < 1){
      // TODO: Change this exception type. Maybe create a new subclass of VeniceException specifically for this case?
      // TODO: Add built-in resilience. Maybe wait and retry, etc.
      throw new VeniceException("No hosts available to serve partition: " + partition +
          ", store: " + resourceName);
    }
    // Proper router will eventually use a strategy other than "read from one"
    String host = instances.get(0).getHost();
    int port = instances.get(0).getPort();
    String keyB64 = Base64.getEncoder().encodeToString(keyBytes);

    CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
    try{
      httpclient.start();
      final HttpGet request = new HttpGet("http://" + host + ":" + port + "/storage/" + resourceName + "/" + partition + "/" + keyB64 + "?f=b64");

      // TODO: Expose async API and let users decide if they wish to block or not.
      HttpResponse response = httpclient.execute(request, null).get(5, TimeUnit.SECONDS); //get is blocking
      InputStream responseContent = response.getEntity().getContent();
      byte[] responseByteArray = IOUtils.toByteArray(responseContent);
      if (response.getStatusLine().getStatusCode() == 404) {
        logger.info("404: " + new String(responseByteArray)); // TODO: Choose appropriate log level (debug or trace?)
        return null; // TODO: Choose whether to throw instead
      } else {
        return valueSerializer.deserialize(resourceName, responseByteArray);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to execute http request to: " + host + " for store: " + resourceName, e);
    } finally {
      try {
        httpclient.close();
      } catch (IOException e) {
        logger.warn("Error closing httpclient", e);
      }
    }
  }
}
