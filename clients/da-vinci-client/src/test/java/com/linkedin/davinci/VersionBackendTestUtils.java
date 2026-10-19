package com.linkedin.davinci;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import java.util.Collections;


public class VersionBackendTestUtils {
  public static Runnable createSubscription(
      DaVinciBackend backend,
      Version version,
      int partition,
      VeniceConfigLoader configLoader,
      StorageService storageService,
      HeartbeatMonitoringService heartbeatService,
      KafkaStoreIngestionService ingestionService) {
    doReturn(configLoader).when(backend).getConfigLoader();
    doReturn(storageService).when(backend).getStorageService();
    doReturn(heartbeatService).when(backend).getHeartbeatMonitoringService();
    doReturn(ingestionService).when(backend).getIngestionService();
    VersionBackend versionBackend = new VersionBackend(backend, version, mock(StoreBackendStats.class));
    return () -> versionBackend.subscribe(ComplementSet.newSet(Collections.singleton(partition)), null, null, false);
  }
}
