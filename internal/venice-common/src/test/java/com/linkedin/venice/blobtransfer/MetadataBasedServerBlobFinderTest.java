package com.linkedin.venice.blobtransfer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataBasedServerBlobFinderTest {
  private static final String STORE_NAME = "testStore";
  private static final int VERSION = 1;
  private static final String DISCOVERY_D2 = "venice-discovery";
  private static final String SERVER_D2 = "venice-server-d2";

  @Test
  public void testExtractReplicasNormalizesUrlEntriesToHosts() {
    // Server metadata populates routing values as instance URLs (https://host:port); the finder returns bare hosts.
    Map<String, List<String>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-x:7778"));
    routingInfo.put("1", Arrays.asList("https://server-a:7778", "https://server-b:7778"));
    Assert.assertEquals(
        MetadataBasedServerBlobFinder.extractReplicas(routingInfo, 1),
        Arrays.asList("server-a", "server-b"));
  }

  @Test
  public void testExtractReplicasEmptyWhenPartitionAbsent() {
    Map<String, List<String>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("hostX"));
    Assert.assertTrue(MetadataBasedServerBlobFinder.extractReplicas(routingInfo, 7).isEmpty());
  }

  @Test
  public void testExtractReplicasEmptyWhenRoutingNull() {
    Assert.assertTrue(MetadataBasedServerBlobFinder.extractReplicas(null, 0).isEmpty());
  }

  @Test
  public void testNormalizeHost() {
    Assert.assertEquals(MetadataBasedServerBlobFinder.normalizeHost("https://server-a:7778"), "server-a");
    Assert.assertEquals(MetadataBasedServerBlobFinder.normalizeHost("http://server-b:1234"), "server-b");
    // Host-only entries pass through unchanged.
    Assert.assertEquals(MetadataBasedServerBlobFinder.normalizeHost("server-c"), "server-c");
    // Bracketed IPv6 literals are unwrapped.
    Assert.assertEquals(MetadataBasedServerBlobFinder.normalizeHost("[::1]"), "::1");
    Assert.assertNull(MetadataBasedServerBlobFinder.normalizeHost(null));
  }

  /**
   * The metadata endpoint is server-served, so the transport must be pointed at the store's SERVER D2 service (resolved
   * via D2 service discovery) rather than the cluster-discovery/Router service. The per-store transport is cached, so
   * discovery runs once.
   */
  @Test
  public void testGetServerTransportClientResolvesAndCachesServerD2() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    when(clientConfig.getD2Client()).thenReturn(mock(D2Client.class));
    when(clientConfig.getD2ServiceName()).thenReturn(DISCOVERY_D2);

    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);
    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setServerD2Service(SERVER_D2);
    when(d2ServiceDiscovery.find(any(D2TransportClient.class), eq(STORE_NAME), eq(true))).thenReturn(discoveryResponse);

    MetadataBasedServerBlobFinder finder = new MetadataBasedServerBlobFinder(clientConfig, d2ServiceDiscovery);

    D2TransportClient transportClient = finder.getServerTransportClient(STORE_NAME);
    Assert.assertEquals(transportClient.getServiceName(), SERVER_D2);

    // Cached: a second lookup returns the same instance and does not re-run discovery.
    Assert.assertSame(finder.getServerTransportClient(STORE_NAME), transportClient);
    verify(d2ServiceDiscovery, times(1)).find(any(D2TransportClient.class), eq(STORE_NAME), eq(true));
  }

  @Test(expectedExceptions = com.linkedin.venice.client.exceptions.VeniceClientException.class)
  public void testGetServerTransportClientFailsWhenServerD2Missing() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    when(clientConfig.getD2Client()).thenReturn(mock(D2Client.class));
    when(clientConfig.getD2ServiceName()).thenReturn(DISCOVERY_D2);

    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);
    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    when(d2ServiceDiscovery.find(any(D2TransportClient.class), eq(STORE_NAME), eq(true))).thenReturn(discoveryResponse);

    new MetadataBasedServerBlobFinder(clientConfig, d2ServiceDiscovery).getServerTransportClient(STORE_NAME);
  }

  @Test
  public void testDiscoverBlobPeersSuccess() {
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(1)).thenReturn(MetadataResponseRecord.SCHEMA$);
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-x:7778"));
    routingInfo.put("1", Arrays.asList("https://server-a:7778", "https://server-b:7778"));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(
        CompletableFuture.completedFuture(new TransportClientResponse(1, null, serializeMetadata(routingInfo))));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 1);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("server-a", "server-b"));

    // The writer schema was resolved from the response's advertised schema id, not assumed to be the compiled one.
    verify(schemaReader).getValueSchema(1);

    // The metadata endpoint is hit by store name (server returns routing for the current serving version).
    ArgumentCaptor<String> uriCaptor = ArgumentCaptor.forClass(String.class);
    verify(transportClient).get(uriCaptor.capture());
    Assert.assertEquals(uriCaptor.getValue(), "metadata/" + STORE_NAME);
  }

  @Test
  public void testConcurrentPartitionDiscoverySharesMetadataRequest() throws Exception {
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(1)).thenReturn(MetadataResponseRecord.SCHEMA$);
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));
    routingInfo.put("1", Arrays.asList("https://server-b:7778"));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    CompletableFuture<TransportClientResponse> metadataResponse = new CompletableFuture<>();
    when(transportClient.get(anyString())).thenReturn(metadataResponse);
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<BlobPeersDiscoveryResponse> partitionZero = executor.submit(() -> discoverAfterLatch(finder, start, 0));
      Future<BlobPeersDiscoveryResponse> partitionOne = executor.submit(() -> discoverAfterLatch(finder, start, 1));
      start.countDown();

      verify(transportClient, timeout(1000).times(1)).get("metadata/" + STORE_NAME);
      metadataResponse.complete(new TransportClientResponse(1, null, serializeMetadata(routingInfo)));

      Assert.assertEquals(partitionZero.get().getDiscoveryResult(), Arrays.asList("server-a"));
      Assert.assertEquals(partitionOne.get().getDiscoveryResult(), Arrays.asList("server-b"));

      finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
      verify(transportClient, times(2)).get("metadata/" + STORE_NAME);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testConcurrentMetadataRequestFailureIsSharedAndNextLookupRetries() throws Exception {
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(1)).thenReturn(MetadataResponseRecord.SCHEMA$);
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    CompletableFuture<TransportClientResponse> failedMetadataResponse = new CompletableFuture<>();
    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));
    when(transportClient.get(anyString())).thenReturn(
        failedMetadataResponse,
        CompletableFuture.completedFuture(new TransportClientResponse(1, null, serializeMetadata(routingInfo))));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicReference<Thread> waitingThread = new AtomicReference<>();
    try {
      Future<BlobPeersDiscoveryResponse> requestOwner =
          executor.submit(() -> finder.discoverBlobPeers(STORE_NAME, VERSION, 0));
      verify(transportClient, timeout(1000)).get("metadata/" + STORE_NAME);

      Future<BlobPeersDiscoveryResponse> sharedRequest = executor.submit(() -> {
        waitingThread.set(Thread.currentThread());
        return finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
      });
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(waitingThread.get().getState(), Thread.State.WAITING));

      failedMetadataResponse.completeExceptionally(new RuntimeException("metadata unavailable"));

      Assert.assertTrue(requestOwner.get().isError());
      Assert.assertTrue(sharedRequest.get().isError());
      verify(transportClient, times(1)).get("metadata/" + STORE_NAME);

      BlobPeersDiscoveryResponse retryResponse = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
      Assert.assertFalse(retryResponse.isError());
      Assert.assertEquals(retryResponse.getDiscoveryResult(), Arrays.asList("server-a"));
      verify(transportClient, times(2)).get("metadata/" + STORE_NAME);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testDiscoverBlobPeersFallsBackToCompiledSchemaWhenSchemaIdInvalid() {
    // No usable writer schema id on the response: the finder must not consult the schema reader and instead decode
    // with the client's compiled schema.
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(
        CompletableFuture.completedFuture(
            new TransportClientResponse(SchemaData.INVALID_VALUE_SCHEMA_ID, null, serializeMetadata(routingInfo))));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("server-a"));
    verify(schemaReader, never()).getValueSchema(anyInt());
  }

  @Test
  public void testDiscoverBlobPeersFallsBackToCompiledSchemaWhenWriterSchemaUnresolved() {
    // The writer schema id is valid but the reader cannot resolve it (e.g. a transient Router issue): fall back to the
    // compiled schema rather than failing discovery.
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(9)).thenThrow(new RuntimeException("schema fetch failed"));
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(
        CompletableFuture.completedFuture(new TransportClientResponse(9, null, serializeMetadata(routingInfo))));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("server-a"));
    verify(schemaReader).getValueSchema(9);
  }

  @Test
  public void testDiscoverBlobPeersErrorsWhenNoD2Client() {
    // No D2 client (non-D2 deployment) -> discovery cannot route to a server -> fail safe to VT replay.
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(STORE_NAME);
    Assert.assertNull(clientConfig.getD2Client());
    MetadataBasedServerBlobFinder finder = new MetadataBasedServerBlobFinder(clientConfig);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);

    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getDiscoveryResult() == null || response.getDiscoveryResult().isEmpty());
  }

  @Test
  public void testDiscoverBlobPeersErrorsWhenDiscoveryFails() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    when(clientConfig.getD2Client()).thenReturn(mock(D2Client.class));
    when(clientConfig.getD2ServiceName()).thenReturn(DISCOVERY_D2);

    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);
    when(d2ServiceDiscovery.find(any(D2TransportClient.class), eq(STORE_NAME), eq(true)))
        .thenThrow(new RuntimeException("d2 discovery unavailable"));

    MetadataBasedServerBlobFinder finder = new MetadataBasedServerBlobFinder(clientConfig, d2ServiceDiscovery);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
    Assert.assertTrue(response.isError());
  }

  @Test
  public void testMetadataRequestFailureTriggersD2Rediscovery() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(1)).thenReturn(MetadataResponseRecord.SCHEMA$);

    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);
    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setServerD2Service(SERVER_D2);
    when(d2ServiceDiscovery.find(any(D2TransportClient.class), eq(STORE_NAME), eq(true))).thenReturn(discoveryResponse);

    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(clientConfig, d2ServiceDiscovery, schemaReader));
    D2TransportClient failedTransport = mock(D2TransportClient.class);
    D2TransportClient recoveredTransport = mock(D2TransportClient.class);
    doReturn(failedTransport, recoveredTransport).when(finder).getD2TransportClient(STORE_NAME);

    CompletableFuture<TransportClientResponse> failedResponse = new CompletableFuture<>();
    failedResponse.completeExceptionally(new RuntimeException("old D2 service unavailable"));
    when(failedTransport.get(anyString())).thenReturn(failedResponse);

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));
    when(recoveredTransport.get(anyString())).thenReturn(
        CompletableFuture.completedFuture(new TransportClientResponse(1, null, serializeMetadata(routingInfo))));

    Assert.assertTrue(finder.discoverBlobPeers(STORE_NAME, VERSION, 0).isError());
    BlobPeersDiscoveryResponse recoveredResponse = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);

    Assert.assertFalse(recoveredResponse.isError());
    Assert.assertEquals(recoveredResponse.getDiscoveryResult(), Arrays.asList("server-a"));
    verify(failedTransport).close();
    verify(d2ServiceDiscovery, times(2)).find(any(D2TransportClient.class), eq(STORE_NAME), eq(true));
  }

  @Test
  public void testMetadataRequestPreservesInterruptedStatus() throws Exception {
    MetadataBasedServerBlobFinder finder = spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class)));
    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(new CompletableFuture<>());
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    AtomicReference<BlobPeersDiscoveryResponse> response = new AtomicReference<>();
    AtomicBoolean interrupted = new AtomicBoolean();
    Thread discoveryThread = new Thread(() -> {
      response.set(finder.discoverBlobPeers(STORE_NAME, VERSION, 0));
      interrupted.set(Thread.currentThread().isInterrupted());
    });

    discoveryThread.start();
    verify(transportClient, timeout(1000)).get("metadata/" + STORE_NAME);
    discoveryThread.interrupt();
    discoveryThread.join();

    Assert.assertTrue(response.get().isError());
    Assert.assertTrue(interrupted.get(), "Interrupted status should be restored before discovery returns");
  }

  @Test
  public void testDiscoverBlobPeersErrorsWhenBodyNull() {
    MetadataBasedServerBlobFinder finder = spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class)));
    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
    Assert.assertTrue(response.isError());
  }

  @Test
  public void testDiscoverBlobPeersErrorsWhenMetadataVersionDiffers() {
    RouterBackedSchemaReader schemaReader = mock(RouterBackedSchemaReader.class);
    when(schemaReader.getValueSchema(1)).thenReturn(MetadataResponseRecord.SCHEMA$);
    MetadataBasedServerBlobFinder finder =
        spy(new MetadataBasedServerBlobFinder(mock(ClientConfig.class), mock(D2ServiceDiscovery.class), schemaReader));

    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    routingInfo.put("0", Arrays.asList("https://server-a:7778"));

    D2TransportClient transportClient = mock(D2TransportClient.class);
    when(transportClient.get(anyString())).thenReturn(
        CompletableFuture.completedFuture(new TransportClientResponse(1, null, serializeMetadata(routingInfo, 2))));
    doReturn(transportClient).when(finder).getServerTransportClient(STORE_NAME);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, 0);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getDiscoveryResult() == null || response.getDiscoveryResult().isEmpty());
  }

  private static byte[] serializeMetadata(Map<CharSequence, List<CharSequence>> routingInfo) {
    return serializeMetadata(routingInfo, VERSION);
  }

  private static byte[] serializeMetadata(Map<CharSequence, List<CharSequence>> routingInfo, int currentVersion) {
    MetadataResponseRecord record = new MetadataResponseRecord(
        new VersionProperties(currentVersion, 0, 1, "", Collections.emptyMap(), 1),
        Collections.singletonList(currentVersion),
        Collections.emptyMap(),
        Collections.emptyMap(),
        1,
        routingInfo,
        Collections.emptyMap(),
        150,
        0);
    return SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$).serialize(record);
  }

  private static BlobPeersDiscoveryResponse discoverAfterLatch(
      MetadataBasedServerBlobFinder finder,
      CountDownLatch start,
      int partitionId) throws InterruptedException {
    start.await();
    return finder.discoverBlobPeers(STORE_NAME, VERSION, partitionId);
  }
}
