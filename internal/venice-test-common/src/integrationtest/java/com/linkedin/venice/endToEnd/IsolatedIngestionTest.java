package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_HEARTBEAT_REQUEST_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.SimpleServer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionTest {
  private static final Logger LOGGER = LogManager.getLogger();

  @Test(timeOut = 300000)
  public void testBackendHandleZombieResource() throws Exception {
    int serverPort = 27015;
    IsolatedIngestionUtils.releaseTargetPortBinding(SimpleServer.class.getName(), serverPort);
    ForkedJavaProcess forkedIngestionProcess = ForkedJavaProcess.exec(
        SimpleServer.class,
        Collections.singletonList(String.valueOf(serverPort)),
        Collections.emptyList(),
        false);
    IsolatedIngestionUtils.waitPortBinding(serverPort, 100);
    VeniceProperties veniceProperties = mock(VeniceProperties.class);
    when(veniceProperties.getInt(SERVER_INGESTION_ISOLATION_HEARTBEAT_REQUEST_TIMEOUT_SECONDS, 5)).thenReturn(5);
    when(veniceProperties.getInt(SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS, 120)).thenReturn(120);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    when(veniceServerConfig.getIngestionServicePort()).thenReturn(serverPort);
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getCombinedProperties()).thenReturn(veniceProperties);
    when(configLoader.getVeniceServerConfig()).thenReturn(veniceServerConfig);

    MainIngestionRequestClient ingestionRequestClient = new MainIngestionRequestClient(configLoader);
    Utils.sleep(1000);
    LOGGER.info(ingestionRequestClient.sendHeartbeatRequest());
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    List<Future<Long>> zombieTopicPartitionFutures = new ArrayList<>();
    List<Future<Long>> goodTopicPartitionFutures = new ArrayList<>();
    int zombieResourceCount = 10;
    int goodResourceCount = 10;
    for (int i = 0; i < zombieResourceCount; i++) {
      zombieTopicPartitionFutures
          .add(simulateHelixStartConsumption(executorService, ingestionRequestClient, "zombie_topic_v" + i, 0));
    }
    Utils.sleep(1000);
    for (int i = 0; i < goodResourceCount; i++) {
      goodTopicPartitionFutures
          .add(simulateHelixStartConsumption(executorService, ingestionRequestClient, "good_topic_v" + i, 0));
    }
    for (int i = 0; i < goodResourceCount; i++) {
      long requestCompletionTime = goodTopicPartitionFutures.get(i).get();
      LOGGER.warn("Good topic {} completed in {} ms", i, requestCompletionTime);
    }
    for (int i = 0; i < zombieResourceCount; i++) {
      long requestCompletionTime = zombieTopicPartitionFutures.get(i).get();
      LOGGER.warn("Zombie topic {} completed in {} ms", i, requestCompletionTime);
    }
    try {
      for (int i = 0; i < goodResourceCount; i++) {
        Assert.assertTrue(goodTopicPartitionFutures.get(i).get() < 1000);
      }
    } finally {
      forkedIngestionProcess.destroy();
    }
  }

  Future<Long> simulateHelixStartConsumption(
      ExecutorService executorService,
      MainIngestionRequestClient client,
      String topic,
      int partition) {
    return executorService.submit(() -> {
      long startTimeMs = System.currentTimeMillis();
      LOGGER.info("Start consumption of topic: {}, partition: {} at {}", topic, partition, startTimeMs);
      long elapsedTimeMs;
      try {
        client.startConsumption(topic, partition);
      } catch (Exception e) {
        LOGGER.warn("Encounter exception when executing start consumption. {}", e.getMessage());
      } finally {
        long finishTime = System.currentTimeMillis();
        elapsedTimeMs = finishTime - startTimeMs;
        LOGGER.info("Completed consumption of topic: {}, partition: {} at {}", topic, partition, finishTime);
      }
      return elapsedTimeMs;
    });
  }
}
