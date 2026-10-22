package com.linkedin.davinci;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;


public class DaVinciPushStatusClusterResolverTest {
  @Test
  public void testPendingDiscoveryDoesNotDelayLegacyHeartbeatsOrDuplicateWork() throws Exception {
    ClusterInfoProvider metadata = mock(ClusterInfoProvider.class);
    when(metadata.getVeniceCluster(anyString())).thenReturn("server-service");
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    PushStatusClusterResolver resolver = new PushStatusClusterResolver(metadata, true, (store, location) -> {
      attempts.incrementAndGet();
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
        throw new VeniceException(e);
      }
      return "cluster";
    });
    VeniceWriterFactory factory = mock(VeniceWriterFactory.class);
    VeniceWriter first = mock(VeniceWriter.class);
    VeniceWriter second = mock(VeniceWriter.class);
    when(factory.createVeniceWriter(any())).thenReturn(first, second);
    Schema valueSchema = AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor();
    try (PushStatusStoreWriter writer = new PushStatusStoreWriter(
        factory,
        "instance",
        new SchemaEntry(1, valueSchema),
        new DerivedSchemaEntry(1, 1, updateSchema),
        resolver)) {
      heartbeatExecutor.submit(() -> writer.writeHeartbeat("first", 1)).get(5, TimeUnit.SECONDS);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      heartbeatExecutor.submit(() -> {
        writer.writeHeartbeat("first", 2);
        writer.writeHeartbeat("second", 3);
        writer.writeHeartbeat("second", 4);
      }).get(5, TimeUnit.SECONDS);
      verify(first, times(2)).update(any(), any(), anyInt(), anyInt(), any());
      verify(second, times(2)).update(any(), any(), anyInt(), anyInt(), any());
      verify(factory, times(2)).createVeniceWriter(any());
      assertEquals(attempts.get(), 1);
      resolver.close();
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertNull(resolver.apply("first"));
    } finally {
      resolver.close();
      release.countDown();
      heartbeatExecutor.shutdownNow();
      assertTrue(heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testLocationChangeSkipsOldClusterIncludingPendingDiscovery() {
    AtomicInteger attempts = new AtomicInteger();
    ResolverFixture fixture =
        new ResolverFixture((store, location) -> attempts.incrementAndGet() == 1 ? "cluster-a" : "cluster-b");
    when(fixture.metadata.getVeniceCluster("first")).thenReturn("service-a");
    when(fixture.metadata.getVeniceCluster("second")).thenReturn("service-b");
    try (PushStatusClusterResolver resolver = fixture.resolver) {
      assertNull(resolver.apply("first"));
      when(fixture.metadata.getVeniceCluster("first")).thenReturn("service-b");
      assertNull(resolver.apply("first"));
      fixture.tasks.remove().run();
      assertNull(resolver.apply("first"));
      assertEquals(fixture.tasks.size(), 1);
      // The pending resolution is shared by stores currently at the same metadata location.
      assertNull(resolver.apply("second"));
      fixture.tasks.remove().run();
      assertEquals(resolver.apply("first"), "cluster-b");
      assertEquals(resolver.apply("second"), "cluster-b");
      when(fixture.time.getMilliseconds()).thenReturn(PushStatusClusterResolver.RETRY_BACKOFF_MS);
      when(fixture.metadata.getVeniceCluster("first")).thenReturn("service-a");
      assertNull(resolver.apply("first"));
      assertEquals(fixture.tasks.size(), 1);
    }
  }

  @Test
  public void testMigratedStoreWithStaleMetadataCannotPoisonSharedLocationCache() {
    ClientConfig config = ClientConfig.defaultGenericClientConfig("first");
    D2TransportClient transport = mock(D2TransportClient.class);
    when(transport.get(anyString(), anyMap())).thenReturn(
        discoveryResponse("cluster-b", "service-b"),
        discoveryResponse("cluster-a", "service-a"),
        discoveryResponse("cluster-b", "service-b"));
    ResolverFixture fixture =
        new ResolverFixture((store, location) -> DaVinciBackend.discoverPushStatusCluster(config, store, location));
    when(fixture.metadata.getVeniceCluster(anyString())).thenReturn("service-a");
    try (MockedStatic<ClientFactory> factory = mockStatic(ClientFactory.class);
        PushStatusClusterResolver resolver = fixture.resolver) {
      factory.when(() -> ClientFactory.getTransportClient(config)).thenReturn(transport);
      assertNull(resolver.apply("first"));
      assertNull(resolver.apply("second"));
      assertEquals(fixture.tasks.size(), 1);
      fixture.tasks.remove().run();
      assertNull(resolver.apply("second"));
      assertTrue(fixture.tasks.isEmpty());
      when(fixture.time.getMilliseconds()).thenReturn(PushStatusClusterResolver.RETRY_BACKOFF_MS);
      assertNull(resolver.apply("second"));
      fixture.tasks.remove().run();
      assertEquals(resolver.apply("second"), "cluster-a");

      when(fixture.metadata.getVeniceCluster("first")).thenReturn("service-b");
      assertNull(resolver.apply("first"));
      fixture.tasks.remove().run();
      assertEquals(resolver.apply("first"), "cluster-b");
      assertEquals(resolver.apply("second"), "cluster-a");
      verify(transport, times(3)).get(anyString(), anyMap());
      verify(transport, times(3)).close();
    }
  }

  @Test
  public void testFailedDiscoveryRetriesOnlyAfterBackoff() {
    AtomicInteger attempts = new AtomicInteger();
    ResolverFixture fixture = new ResolverFixture((store, location) -> {
      if (attempts.incrementAndGet() == 1) {
        throw new VeniceException("discovery unavailable");
      }
      return "cluster";
    });
    try (PushStatusClusterResolver resolver = fixture.resolver) {
      assertNull(resolver.apply("store"));
      fixture.tasks.remove().run();
      assertNull(resolver.apply("store"));
      when(fixture.time.getMilliseconds()).thenReturn(PushStatusClusterResolver.RETRY_BACKOFF_MS - 1);
      assertNull(resolver.apply("store"));
      assertTrue(fixture.tasks.isEmpty());
      when(fixture.time.getMilliseconds()).thenReturn(PushStatusClusterResolver.RETRY_BACKOFF_MS);
      assertNull(resolver.apply("store"));
      assertNull(resolver.apply("store"));
      assertEquals(fixture.tasks.size(), 1);
      fixture.tasks.remove().run();
      assertEquals(resolver.apply("store"), "cluster");
      assertEquals(attempts.get(), 2);
    }
  }

  @Test
  public void testRejectedDiscoveryIsThrottledAndCloseIsIdempotent() {
    ResolverFixture fixture = new ResolverFixture((store, location) -> "cluster");
    doThrow(new RejectedExecutionException("busy")).when(fixture.executor).execute(any());
    assertNull(fixture.resolver.apply("store"));
    assertNull(fixture.resolver.apply("store"));
    verify(fixture.executor).execute(any());
    when(fixture.time.getMilliseconds()).thenReturn(PushStatusClusterResolver.RETRY_BACKOFF_MS);
    assertNull(fixture.resolver.apply("store"));
    verify(fixture.executor, times(2)).execute(any());
    fixture.resolver.close();
    fixture.resolver.close();
    assertNull(fixture.resolver.apply("store"));
    verify(fixture.executor).shutdownNow();
  }

  @Test
  public void testCloseDiscardsQueuedDiscoveryResult() {
    ResolverFixture fixture = new ResolverFixture((store, location) -> "cluster");
    assertNull(fixture.resolver.apply("store"));
    fixture.resolver.close();
    fixture.tasks.remove().run();
    assertNull(fixture.resolver.apply("store"));
    verify(fixture.executor).shutdownNow();
  }

  @Test
  public void testNonRequestMetadataUsesCurrentClusterWithoutDiscovery() {
    ClusterInfoProvider metadata = mock(ClusterInfoProvider.class);
    ClientConfig config = ClientConfig.defaultGenericClientConfig("store").setVeniceURL("http://localhost:7777");
    when(metadata.getVeniceCluster("store")).thenReturn("cluster-a", "cluster-b", null, "");
    try (MockedStatic<ClientFactory> factory = mockStatic(ClientFactory.class);
        PushStatusClusterResolver resolver = DaVinciBackend.createPushStatusClusterResolver(metadata, config, false)) {
      assertEquals(resolver.apply("store"), "cluster-a");
      assertEquals(resolver.apply("store"), "cluster-b");
      assertThrows(VeniceException.class, () -> resolver.apply("store"));
      assertThrows(VeniceException.class, () -> resolver.apply("store"));
      factory.verifyNoInteractions();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDiscoveryValidatesClusterDoesNotRetryAndPreservesD2Ownership(boolean callerOwnedD2Client)
      throws Exception {
    ClientConfig config = ClientConfig.defaultGenericClientConfig("store");
    D2Client sharedClient = mock(D2Client.class);
    D2TransportClient transport = callerOwnedD2Client
        ? spy(new D2TransportClient("router-service", sharedClient))
        : mock(D2TransportClient.class);
    CompletableFuture<TransportClientResponse> failure = new CompletableFuture<>();
    failure.completeExceptionally(new VeniceException("unavailable"));
    doReturn(discoveryResponse("cluster"), discoveryResponse(""), failure).when(transport).get(anyString(), anyMap());
    try (MockedStatic<ClientFactory> factory = mockStatic(ClientFactory.class)) {
      factory.when(() -> ClientFactory.getTransportClient(config)).thenReturn(transport);
      assertEquals(DaVinciBackend.discoverPushStatusCluster(config, "store", "server-service"), "cluster");
      assertThrows(
          VeniceException.class,
          () -> DaVinciBackend.discoverPushStatusCluster(config, "store", "server-service"));
      verify(transport, times(2)).get(anyString(), anyMap());
      verify(transport, times(2)).close();
      assertThrows(
          VeniceException.class,
          () -> DaVinciBackend.discoverPushStatusCluster(config, "store", "server-service"));
      verify(transport, times(3)).get(anyString(), anyMap());
      verify(transport, times(3)).close();
      verify(sharedClient, never()).shutdown(any());
      TransportClient nonD2 = mock(TransportClient.class);
      factory.when(() -> ClientFactory.getTransportClient(config)).thenReturn(nonD2);
      assertThrows(
          VeniceException.class,
          () -> DaVinciBackend.discoverPushStatusCluster(config, "store", "server-service"));
      verify(nonD2).close();
    }
  }

  private static class ResolverFixture {
    final ClusterInfoProvider metadata = mock(ClusterInfoProvider.class);
    final ExecutorService executor = mock(ExecutorService.class);
    final Time time = mock(Time.class);
    final Queue<Runnable> tasks = new ArrayDeque<>();
    final PushStatusClusterResolver resolver;

    ResolverFixture(BiFunction<String, String, String> discovery) {
      when(metadata.getVeniceCluster(anyString())).thenReturn("server-service");
      doAnswer(invocation -> {
        tasks.add(invocation.getArgument(0));
        return null;
      }).when(executor).execute(any());
      resolver = new PushStatusClusterResolver(metadata, discovery, executor, time);
    }
  }

  private CompletableFuture<TransportClientResponse> discoveryResponse(String cluster) {
    return discoveryResponse(cluster, "server-service");
  }

  private CompletableFuture<TransportClientResponse> discoveryResponse(String cluster, String serverService) {
    return CompletableFuture.completedFuture(
        new TransportClientResponse(
            0,
            CompressionStrategy.NO_OP,
            ("{\"cluster\":\"" + cluster + "\",\"d2Service\":\"router-service\",\"serverD2Service\":\"" + serverService
                + "\"}").getBytes(StandardCharsets.UTF_8)));
  }
}
