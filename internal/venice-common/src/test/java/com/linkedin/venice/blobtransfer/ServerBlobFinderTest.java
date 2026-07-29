package com.linkedin.venice.blobtransfer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Version;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerBlobFinderTest {
  private HelixCustomizedViewOfflinePushRepository mockCustomizedViewRepository;

  @BeforeMethod
  public void setUp() {
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
  }

  @Test
  public void testFindBlob() {
    // Arrange
    String storeName = "test-store";
    int version = 2;
    int partitionId = 0;
    String topicName = Version.composeKafkaTopic(storeName, version);
    Instance instance2 = new Instance("host2", "host2", 1234);
    List<Instance> readyToServeInstances = Collections.singletonList(instance2);
    doReturn(readyToServeInstances).when(mockCustomizedViewRepository).getReadyToServeInstances(topicName, partitionId);
    // Act
    ServerBlobFinder serverBlobFinder =
        new ServerBlobFinder(CompletableFuture.completedFuture(mockCustomizedViewRepository));
    BlobPeersDiscoveryResponse resultBlobResponse = serverBlobFinder.discoverBlobPeers(storeName, version, partitionId);

    // Assert
    Assert.assertNotNull(resultBlobResponse);
    Assert.assertEquals(resultBlobResponse.getDiscoveryResult().size(), 1);
    Assert.assertEquals(resultBlobResponse.getDiscoveryResult().get(0), instance2.getHost());
  }
}
