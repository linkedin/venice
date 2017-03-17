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


public class KafkaStoreIngestionServiceTest {
  private StoreRepository mockStoreRepository;
  private VeniceConfigLoader mockVeniceConfigLoader;
  private OffsetManager mockOffsetManager;
  private ReadOnlyStoreRepository mockmetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;

  private KafkaStoreIngestionService kafkaStoreIngestionService;

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
   kafkaStoreIngestionService = new KafkaStoreIngestionService(mockStoreRepository,
                                                                   mockVeniceConfigLoader,
                                                                   mockOffsetManager,
                                                                   mockmetadataRepo,
                                                                   mockSchemaRepo,
                                                                   new MetricsRepository());

   String mockStoreName = "test";
   String mockSimilarStoreName = "testTest";
   int taskNum = 3;
   Map<String, StoreIngestionTask> topicNameToIngestionTaskMap = Collections.synchronizedMap(new HashMap<> ());

   for (int i = 1; i <= taskNum; i ++ ) {
     StoreIngestionTask task = mock(StoreIngestionTask.class);
     topicNameToIngestionTaskMap.put(mockStoreName + "_v" + String.valueOf(i), task);
   }

   topicNameToIngestionTaskMap.put(mockSimilarStoreName + "_v1", mock(StoreIngestionTask.class));

   Store mockStore = mock(Store.class);
   doReturn(3).when(mockStore).getLargestUsedVersionNumber();
   doReturn(mockStore).when(mockmetadataRepo).getStore(mockStoreName);

   VeniceStoreConfig mockStoreConfig = mock(VeniceStoreConfig.class);
   doReturn(mockStoreName + "_v" + String.valueOf(taskNum)).when(mockStoreConfig).getStoreName();

   kafkaStoreIngestionService.disableOldTopicMetricsEmission(topicNameToIngestionTaskMap, mockStoreName, 3);


   String mostRecentTopic = mockStoreName + "_v" +taskNum;
   topicNameToIngestionTaskMap.forEach((topicName, task) -> {
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
