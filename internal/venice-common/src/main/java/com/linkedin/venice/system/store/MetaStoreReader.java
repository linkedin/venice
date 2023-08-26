package com.linkedin.venice.system.store;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MetaStoreReader implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(MetaStoreReader.class);
  private static final int DEFAULT_HEARTBEAT_READ_TIMEOUT_SECONDS = 3;
  private final Map<String, AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue>> veniceClients =
      new VeniceConcurrentHashMap<>();
  private final D2Client d2Client;
  private final String clusterDiscoveryD2ServiceName;

  public MetaStoreReader(D2Client d2Client, String clusterDiscoveryD2ServiceName) {
    this.d2Client = d2Client;
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
  }

  public long getHeartbeat(String storeName) {
    AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> client = getVeniceClient(storeName);
    StoreMetaKey key =
        MetaStoreDataType.HEARTBEAT.getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    try {
      StoreMetaValue value = client.get(key).get(DEFAULT_HEARTBEAT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (value == null) {
        return 0;
      } else {
        return value.timestamp;
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> getVeniceClient(String storeName) {
    return veniceClients.computeIfAbsent(storeName, (s) -> {
      ClientConfig clientConfig =
          ClientConfig.defaultGenericClientConfig(VeniceSystemStoreUtils.getMetaStoreName(storeName))
              .setD2Client(d2Client)
              .setD2ServiceName(clusterDiscoveryD2ServiceName)
              .setSpecificValueClass(StoreMetaValue.class);
      return ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    });
  }

  @Override
  public void close() {
    veniceClients.forEach((storeName, veniceClient) -> {
      try {
        veniceClient.close();
      } catch (Exception e) {
        LOGGER.error("Can not close VeniceClient.", e);
      }
    });
  }

}
