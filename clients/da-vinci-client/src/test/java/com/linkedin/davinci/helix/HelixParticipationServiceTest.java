package com.linkedin.davinci.helix;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class HelixParticipationServiceTest {
  private static final Logger LOGGER = LogManager.getLogger(HelixParticipationServiceTest.class);

  @Test
  public void testRestAllInstanceCVStates() {
    HelixPartitionStatusAccessor mockAccessor = mock(HelixPartitionStatusAccessor.class);
    StorageService mockStorageService = mock(StorageService.class);
    String resourceV1 = "test_resource_v1";
    String resourceV2 = "test_resource_v2";
    Set<Integer> partitionSet = new HashSet<>(Arrays.asList(1, 2, 3));
    Map<String, Set<Integer>> storePartitionMapping = new HashMap<>();
    storePartitionMapping.put(resourceV1, partitionSet);
    storePartitionMapping.put(resourceV2, partitionSet);
    doReturn(storePartitionMapping).when(mockStorageService).getStoreAndUserPartitionsMapping();

    HelixParticipationService.resetAllInstanceCVStates(mockAccessor, mockStorageService, LOGGER);

    verify(mockAccessor).deleteReplicaStatus(resourceV1, 1);
    verify(mockAccessor).deleteReplicaStatus(resourceV1, 2);
    verify(mockAccessor).deleteReplicaStatus(resourceV1, 3);
    verify(mockAccessor).deleteReplicaStatus(resourceV2, 1);
    verify(mockAccessor).deleteReplicaStatus(resourceV2, 2);
    verify(mockAccessor).deleteReplicaStatus(resourceV2, 3);
  }

  @Test
  public void testUnknownHelixInstanceOperation() {
    HelixParticipationService mockHelixParticipationService = mock(HelixParticipationService.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);

    when(mockServerConfig.isHelixJoinAsUnknownEnabled()).thenReturn(true);

    doCallRealMethod().when(mockHelixParticipationService).buildHelixManagerProperty(mockServerConfig);
    mockHelixParticipationService.buildHelixManagerProperty(mockServerConfig);
  }
}
