package com.linkedin.venice.ingestion.control;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.VeniceResourceCloseResult;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiRegionRealTimeTopicSwitcherTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test(timeOut = 5000)
  public void testBroadcastVersionSwapWithRegionInfoToAllDataCenters() {
    // Arrange inputs
    String storeName = "TestStore";
    String localDc = "dc_local";
    String remoteDcA = "dc_a";
    String remoteDcB = "dc_b";

    TopicManager mockTopicManager = mock(TopicManager.class);

    // Single factory and distinct writers selected by brokerAddress
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceWriter localWriter = mock(VeniceWriter.class);
    VeniceWriter remoteWriterA = mock(VeniceWriter.class);
    VeniceWriter remoteWriterB = mock(VeniceWriter.class);
    CountDownLatch allRegionsStartedBroadcasting = new CountDownLatch(3);
    when(writerFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenAnswer(invocation -> {
      VeniceWriterOptions opts = invocation.getArgument(0);
      String broker = opts.getBrokerAddress();
      if (broker == null) {
        return localWriter;
      }
      switch (broker) {
        case "broker-a":
          return remoteWriterA;
        case "broker-b":
          return remoteWriterB;
        default:
          return localWriter;
      }
    });
    // Ensure nonBlockingBroadcast returns completed futures so switcher does not block
    when(localWriter.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenAnswer(invocation -> awaitAllRegionsAndReturnCompletedFuture(allRegionsStartedBroadcasting));
    when(remoteWriterA.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenAnswer(invocation -> awaitAllRegionsAndReturnCompletedFuture(allRegionsStartedBroadcasting));
    when(remoteWriterB.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenAnswer(invocation -> awaitAllRegionsAndReturnCompletedFuture(allRegionsStartedBroadcasting));
    CompletableFuture<VeniceResourceCloseResult> localCloseFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResourceCloseResult> remoteCloseFutureA = new CompletableFuture<>();
    CompletableFuture<VeniceResourceCloseResult> remoteCloseFutureB = new CompletableFuture<>();
    AtomicInteger initiatedCloseCount = new AtomicInteger();
    when(localWriter.closeAsync(true)).thenAnswer(invocation -> {
      completeCloseFuturesAfterAllWritersStartClosing(
          initiatedCloseCount,
          localCloseFuture,
          remoteCloseFutureA,
          remoteCloseFutureB);
      return localCloseFuture;
    });
    when(remoteWriterA.closeAsync(true)).thenAnswer(invocation -> {
      completeCloseFuturesAfterAllWritersStartClosing(
          initiatedCloseCount,
          localCloseFuture,
          remoteCloseFutureA,
          remoteCloseFutureB);
      return remoteCloseFutureA;
    });
    when(remoteWriterB.closeAsync(true)).thenAnswer(invocation -> {
      completeCloseFuturesAfterAllWritersStartClosing(
          initiatedCloseCount,
          localCloseFuture,
          remoteCloseFutureA,
          remoteCloseFutureB);
      return remoteCloseFutureB;
    });

    Properties props = new Properties();
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "dummy");
    VeniceProperties veniceProperties = new VeniceProperties(props);

    // Build versions and store
    Version prevVersion = mock(Version.class, RETURNS_DEEP_STUBS);
    Version nextVersion = mock(Version.class, RETURNS_DEEP_STUBS);
    when(prevVersion.getStoreName()).thenReturn(storeName);
    when(nextVersion.getStoreName()).thenReturn(storeName);
    when(prevVersion.getNumber()).thenReturn(1);
    when(nextVersion.getNumber()).thenReturn(2);
    when(prevVersion.isHybrid()).thenReturn(true);
    when(nextVersion.isHybrid()).thenReturn(true);
    when(prevVersion.getPartitionCount()).thenReturn(8);
    when(nextVersion.getPartitionCount()).thenReturn(12);

    String prevTopic = Version.composeKafkaTopic(storeName, 1);
    String nextTopic = Version.composeKafkaTopic(storeName, 2);
    when(prevVersion.kafkaTopicName()).thenReturn(prevTopic);
    when(nextVersion.kafkaTopicName()).thenReturn(nextTopic);

    // RT topic exists check needs repo + topic manager
    String rtTopicName = Utils.getRealTimeTopicName(prevVersion);
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(rtTopicName);
    when(mockTopicManager.containsTopic(rtTopic)).thenReturn(true);

    // Store with versions
    Store mockStore = mock(Store.class);
    when(mockStore.getVersionOrThrow(1)).thenReturn(prevVersion);
    when(mockStore.getVersionOrThrow(2)).thenReturn(nextVersion);

    // Build broker map for all (including local); spy the switcher to make generation id deterministic
    long deterministicGenerationId = 12345L;
    Map<String, String> brokerMap = new HashMap<>();
    brokerMap.put(localDc, "broker-local");
    brokerMap.put(remoteDcA, "broker-a");
    brokerMap.put(remoteDcB, "broker-b");
    MultiRegionRealTimeTopicSwitcher switcher = spy(
        new MultiRegionRealTimeTopicSwitcher(
            mockTopicManager,
            writerFactory,
            veniceProperties,
            pubSubTopicRepository,
            brokerMap,
            localDc));
    doReturn(deterministicGenerationId).when(switcher).getVersionSwapGenerationId();

    // Act: trigger transmitVersionSwapMessage which will delegate to overridden broadcastVersionSwap
    switcher.transmitVersionSwapMessage(mockStore, 1, 2);

    // Assert: verify VeniceWriterOptions used expected topic and partition count
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);
    // Total calls = number of DCs (local + 2 remotes) => 3
    verify(writerFactory, times(3)).createVeniceWriter(optionsCaptor.capture());

    for (VeniceWriterOptions vwo: optionsCaptor.getAllValues()) {
      Assert.assertEquals(vwo.getTopicName(), rtTopicName, "Topic name should be RT topic");
      Assert.assertEquals(vwo.getPartitionCount().intValue(), 12, "Partition count should be next version's");
    }

    // Verify each writer received a region-aware version swap with expected arguments
    verify(localWriter, times(1)).nonBlockingBroadcastVersionSwapWithRegionInfo(
        prevTopic,
        nextTopic,
        localDc,
        localDc,
        deterministicGenerationId,
        Collections.EMPTY_MAP);

    verify(remoteWriterA, times(1)).nonBlockingBroadcastVersionSwapWithRegionInfo(
        prevTopic,
        nextTopic,
        localDc,
        remoteDcA,
        deterministicGenerationId,
        Collections.EMPTY_MAP);

    verify(remoteWriterB, times(1)).nonBlockingBroadcastVersionSwapWithRegionInfo(
        prevTopic,
        nextTopic,
        localDc,
        remoteDcB,
        deterministicGenerationId,
        Collections.EMPTY_MAP);

    verify(localWriter).closeAsync(true);
    verify(remoteWriterA).closeAsync(true);
    verify(remoteWriterB).closeAsync(true);
    verify(localWriter, never()).closeAsync(false);
    verify(remoteWriterA, never()).closeAsync(false);
    verify(remoteWriterB, never()).closeAsync(false);
  }

  @Test(timeOut = 5000)
  public void testFailedPartitionBroadcastReportsAllRegionsAndCleansUpWriters() {
    String localDc = "dc_local";
    String remoteDcA = "dc_a";
    String remoteDcB = "dc_b";
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceWriter remoteWriterA = mock(VeniceWriter.class);
    VeniceWriter remoteWriterB = mock(VeniceWriter.class);
    CountDownLatch allRegionsStartedBroadcasting = new CountDownLatch(2);
    when(writerFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenAnswer(invocation -> {
      String broker = ((VeniceWriterOptions) invocation.getArgument(0)).getBrokerAddress();
      return "broker-a".equals(broker) ? remoteWriterA : remoteWriterB;
    });
    when(remoteWriterA.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenAnswer(invocation -> awaitAllRegionsAndReturnFailedFuture(allRegionsStartedBroadcasting));
    when(remoteWriterB.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenAnswer(invocation -> awaitAllRegionsAndReturnFailedFuture(allRegionsStartedBroadcasting));
    when(remoteWriterA.closeAsync(false))
        .thenReturn(CompletableFuture.completedFuture(VeniceResourceCloseResult.SUCCESS));
    when(remoteWriterB.closeAsync(false))
        .thenReturn(CompletableFuture.completedFuture(VeniceResourceCloseResult.SUCCESS));

    Map<String, String> brokerMap = new HashMap<>();
    brokerMap.put(remoteDcA, "broker-a");
    brokerMap.put(remoteDcB, "broker-b");
    MultiRegionRealTimeTopicSwitcher switcher =
        newSwitcher(mock(TopicManager.class), writerFactory, brokerMap, localDc);
    Version previousVersion = version("TestStore", 1, 8);
    Version nextVersion = version("TestStore", 2, 12);

    VeniceException exception = Assert.expectThrows(
        VeniceException.class,
        () -> switcher.broadcastVersionSwap(previousVersion, nextVersion, "TestStore_rt"));

    Assert.assertTrue(exception.getMessage().contains(remoteDcA));
    Assert.assertTrue(exception.getMessage().contains(remoteDcB));
    verify(remoteWriterA).closeAsync(false);
    verify(remoteWriterB).closeAsync(false);
    verify(remoteWriterA, never()).closeAsync(true);
    verify(remoteWriterB, never()).closeAsync(true);
  }

  @Test
  public void testGracefulCloseFailureDoesNotFailCompletedBroadcast() {
    String localDc = "dc_local";
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceWriter writer = mock(VeniceWriter.class);
    when(writerFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenReturn(writer);
    when(writer.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(mock(PubSubProduceResult.class))));
    CompletableFuture<VeniceResourceCloseResult> failedClose = new CompletableFuture<>();
    failedClose.completeExceptionally(new VeniceException("close failed"));
    when(writer.closeAsync(true)).thenReturn(failedClose);
    when(writer.closeAsync(false)).thenReturn(CompletableFuture.completedFuture(VeniceResourceCloseResult.SUCCESS));

    MultiRegionRealTimeTopicSwitcher switcher = newSwitcher(
        mock(TopicManager.class),
        writerFactory,
        Collections.singletonMap(localDc, "broker-local"),
        localDc);

    switcher.broadcastVersionSwap(version("TestStore", 1, 8), version("TestStore", 2, 12), "TestStore_rt");

    verify(writer).closeAsync(true);
    verify(writer).closeAsync(false);
  }

  @Test(timeOut = 5000)
  public void testGracefulCloseTimeoutDoesNotFailCompletedBroadcast() throws Exception {
    String localDc = "dc_local";
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    VeniceWriter writer = mock(VeniceWriter.class);
    when(writerFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenReturn(writer);
    when(writer.nonBlockingBroadcastVersionSwapWithRegionInfo(any(), any(), any(), any(), anyLong(), any()))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(mock(PubSubProduceResult.class))));
    CompletableFuture<VeniceResourceCloseResult> neverCompletingClose = new CompletableFuture<>();
    when(writer.closeAsync(true)).thenReturn(neverCompletingClose);
    when(writer.closeAsync(false)).thenReturn(CompletableFuture.completedFuture(VeniceResourceCloseResult.SUCCESS));

    MultiRegionRealTimeTopicSwitcher switcher = spy(
        newSwitcher(
            mock(TopicManager.class),
            writerFactory,
            Collections.singletonMap(localDc, "broker-local"),
            localDc));
    doReturn(100L).when(switcher).getRemainingTimeInMs(anyLong());

    switcher.broadcastVersionSwap(version("TestStore", 1, 8), version("TestStore", 2, 12), "TestStore_rt");

    Assert.assertFalse(neverCompletingClose.isDone());
    verify(writer).closeAsync(true);
    verify(writer).closeAsync(false);
  }

  @Test
  public void testWriterCreationFailureDoesNotAttemptCleanup() {
    String localDc = "dc_local";
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    doThrow(new VeniceException("writer creation failed")).when(writerFactory)
        .createVeniceWriter(any(VeniceWriterOptions.class));
    MultiRegionRealTimeTopicSwitcher switcher = newSwitcher(
        mock(TopicManager.class),
        writerFactory,
        Collections.singletonMap(localDc, "broker-local"),
        localDc);

    VeniceException exception = Assert.expectThrows(
        VeniceException.class,
        () -> switcher.broadcastVersionSwap(version("TestStore", 1, 8), version("TestStore", 2, 12), "TestStore_rt"));

    Assert.assertTrue(exception.getMessage().contains(localDc));
  }

  @Test
  public void testEmptyDataCenterMapAndPreviousVersionPartitionCount() {
    VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    Version previousVersion = version("TestStore", 1, 8);
    MultiRegionRealTimeTopicSwitcher switcher =
        newSwitcher(mock(TopicManager.class), writerFactory, Collections.emptyMap(), "dc_local");

    switcher.broadcastVersionSwap(previousVersion, version("TestStore", 2, 12), previousVersion.kafkaTopicName());

    verify(writerFactory, never()).createVeniceWriter(any(VeniceWriterOptions.class));
  }

  @Test
  public void testRemainingBroadcastDeadline() throws Exception {
    MultiRegionRealTimeTopicSwitcher switcher =
        newSwitcher(mock(TopicManager.class), mock(VeniceWriterFactory.class), Collections.emptyMap(), "dc_local");

    Assert.assertTrue(switcher.getRemainingTimeInMs(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)) > 0);
    Assert.expectThrows(TimeoutException.class, () -> switcher.getRemainingTimeInMs(System.nanoTime() - 1));
  }

  private MultiRegionRealTimeTopicSwitcher newSwitcher(
      TopicManager topicManager,
      VeniceWriterFactory writerFactory,
      Map<String, String> brokerMap,
      String localDc) {
    Properties props = new Properties();
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "dummy");
    return new MultiRegionRealTimeTopicSwitcher(
        topicManager,
        writerFactory,
        new VeniceProperties(props),
        pubSubTopicRepository,
        brokerMap,
        localDc);
  }

  private static Version version(String storeName, int number, int partitionCount) {
    Version version = mock(Version.class);
    when(version.getStoreName()).thenReturn(storeName);
    when(version.getNumber()).thenReturn(number);
    when(version.getPartitionCount()).thenReturn(partitionCount);
    when(version.kafkaTopicName()).thenReturn(Version.composeKafkaTopic(storeName, number));
    return version;
  }

  private static void completeCloseFuturesAfterAllWritersStartClosing(
      AtomicInteger initiatedCloseCount,
      CompletableFuture<VeniceResourceCloseResult>... closeFutures) {
    if (initiatedCloseCount.incrementAndGet() == closeFutures.length) {
      Arrays.stream(closeFutures).forEach(future -> future.complete(VeniceResourceCloseResult.SUCCESS));
    }
  }

  private static java.util.List<CompletableFuture<PubSubProduceResult>> awaitAllRegionsAndReturnCompletedFuture(
      CountDownLatch allRegionsStartedBroadcasting) throws InterruptedException {
    allRegionsStartedBroadcasting.countDown();
    Assert.assertTrue(
        allRegionsStartedBroadcasting.await(2, TimeUnit.SECONDS),
        "All regional broadcasts should start concurrently");
    return Collections.singletonList(CompletableFuture.completedFuture(mock(PubSubProduceResult.class)));
  }

  private static java.util.List<CompletableFuture<PubSubProduceResult>> awaitAllRegionsAndReturnFailedFuture(
      CountDownLatch allRegionsStartedBroadcasting) throws InterruptedException {
    allRegionsStartedBroadcasting.countDown();
    Assert.assertTrue(
        allRegionsStartedBroadcasting.await(2, TimeUnit.SECONDS),
        "All regional broadcasts should start concurrently");
    CompletableFuture<PubSubProduceResult> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new VeniceException("partition write failed"));
    return Collections.singletonList(failedFuture);
  }
}
