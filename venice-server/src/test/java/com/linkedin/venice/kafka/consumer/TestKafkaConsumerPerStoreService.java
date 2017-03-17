package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import edu.emory.mathcs.backport.java.util.Collections;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestKafkaConsumerPerStoreService {
  private StoreRepository mockStoreRepository;
  private VeniceConfigLoader mockVeniceConfigLoader;
  private OffsetManager mockOffsetManager;
  private ReadOnlyStoreRepository mockmetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;

  private KafkaConsumerPerStoreService kafkaConsumerPerStoreService;

  @BeforeClass
  public void setup() {
    mockStoreRepository = mock(StoreRepository.class);
    mockOffsetManager = mock(OffsetManager.class);
    mockmetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);

    setupMockConfig();
  }

  private void setupMockConfig() {
    mockVeniceConfigLoader = mock(VeniceConfigLoader.class);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(0l).when(mockVeniceServerConfig).getMaxKafkaFetchBytesPerSecond();

    VeniceClusterConfig mockVeniceClusterConfig = mock(VeniceClusterConfig.class);
    doReturn("localhost:1234").when(mockVeniceClusterConfig).getKafkaZkAddress();

    doReturn(mockVeniceServerConfig).when(mockVeniceConfigLoader).getVeniceServerConfig();
    doReturn(mockVeniceClusterConfig).when(mockVeniceConfigLoader).getVeniceClusterConfig();
  }


 @Test
 public void testDisableMetricsEmission() {
   kafkaConsumerPerStoreService = new KafkaConsumerPerStoreService(mockStoreRepository,
                                                                   mockVeniceConfigLoader,
                                                                   mockOffsetManager,
                                                                   mockmetadataRepo,
                                                                   mockSchemaRepo,
                                                                   new MetricsRepository());

   String mockStoreName = "test";
   String mockSimilarStoreName = "testTest";
   int taskNum = 3;
   Map<String, StoreConsumptionTask> topicNameToConsumptionTaskMap = Collections.synchronizedMap(new HashMap<> ());

   for (int i = 1; i <= taskNum; i ++ ) {
     StoreConsumptionTask task = mock(StoreConsumptionTask.class);
     topicNameToConsumptionTaskMap.put(mockStoreName + "_v" + String.valueOf(i), task);
   }

   topicNameToConsumptionTaskMap.put(mockSimilarStoreName + "_v1", mock(StoreConsumptionTask.class));

   Store mockStore = mock(Store.class);
   doReturn(3).when(mockStore).getLargestUsedVersionNumber();
   doReturn(mockStore).when(mockmetadataRepo).getStore(mockStoreName);

   VeniceStoreConfig mockStoreConfig = mock(VeniceStoreConfig.class);
   doReturn(mockStoreName + "_v" + String.valueOf(taskNum)).when(mockStoreConfig).getStoreName();

   kafkaConsumerPerStoreService.disableOldTopicMetricsEmission(topicNameToConsumptionTaskMap, mockStoreName, 3);


   String mostRecentTopic = mockStoreName + "_v" +taskNum;
   topicNameToConsumptionTaskMap.forEach((topicName, task) -> {
     if (Version.parseStoreFromKafkaTopicName(topicName).equals(mockStoreName)) {
       if (topicName.equals(mostRecentTopic)) {
         verify(task).enableMetricsEmission();
       } else {
         verify(task).disableMetricsEmission();
       }
     } else { //checks store with similar name will not be call
       verify(task, never()).enableMetricsEmission();
       verify(task, never()).disableMetricsEmission();
     }
   });
 }
}
