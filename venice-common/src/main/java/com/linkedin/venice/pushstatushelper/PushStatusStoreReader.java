package com.linkedin.venice.pushstatushelper;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * PushStatusStoreReader is a helper class for Venice controller reading PushStatus/Heartbeat messages.
 * One PushStatusStoreReader for one regular Venice store.
 * Don't keep a map of [storeName->client] to minimize states kept by controller.
 */
public class PushStatusStoreReader implements Closeable {
  private static final Logger logger = LogManager.getLogger(PushStatusStoreReader.class);
  private final Map<String, AvroSpecificStoreClient<PushStatusKey, PushStatusValue>> veniceClients = new VeniceConcurrentHashMap<>();
  private final D2Client d2Client;
  private final long heartbeatExpirationTimeInSeconds;

  public PushStatusStoreReader(D2Client d2Client, long heartbeatExpirationTimeInSeconds) {
    this.d2Client = d2Client;
    this.heartbeatExpirationTimeInSeconds = heartbeatExpirationTimeInSeconds;
  }

  public Map<CharSequence, Integer> getPartitionStatus(String storeName, int version, int partitionId,
      Optional<String> incrementalPushVersion) {
    return getPartitionStatus(storeName, version, partitionId, incrementalPushVersion, Optional.empty());
  }

  public Map<CharSequence, Integer> getPartitionStatus(String storeName, int version, int partitionId,
      Optional<String> incrementalPushVersion, Optional<String> incrementalPushPrefix) {
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = getVeniceClient(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion, incrementalPushPrefix);
    try {
      PushStatusValue pushStatusValue = client.get(pushStatusKey).get();
      if (pushStatusValue == null) {
        return Collections.emptyMap();
      } else {
        logger.info(storeName + "/" + partitionId + " Instance Statuses: " + pushStatusValue.instances);
        return pushStatusValue.instances;
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  /**
   * @param instanceName = [hostname + appName]
   */
  public long getHeartbeat(String storeName, String instanceName) {
    AvroSpecificStoreClient<PushStatusKey, PushStatusValue> client = getVeniceClient(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    try {
      PushStatusValue pushStatusValue = client.get(pushStatusKey).get();
      if (pushStatusValue == null) {
        return 0;
      } else {
        return pushStatusValue.reportTimestamp;
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public boolean isInstanceAlive(String storeName, String instanceName) {
    long lastReportTimeStamp = getHeartbeat(storeName, instanceName);
    return System.currentTimeMillis() - lastReportTimeStamp <= TimeUnit.SECONDS.toMillis(heartbeatExpirationTimeInSeconds);
  }

  private AvroSpecificStoreClient<PushStatusKey, PushStatusValue> getVeniceClient(String storeName) {
    return veniceClients.computeIfAbsent(storeName, (s) -> {
      ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName))
          .setD2Client(d2Client)
          .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
          .setSpecificValueClass(PushStatusValue.class);
     return ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    });
  }

  @Override
  public void close() {
    veniceClients.forEach((storeName, veniceClient) -> {
      try {
        veniceClient.close();
      } catch (Exception e) {
        logger.error("Can not close VeniceClient. ", e);
      }
    });
  }

}
