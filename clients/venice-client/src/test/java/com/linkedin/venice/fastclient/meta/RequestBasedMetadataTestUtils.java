package com.linkedin.venice.fastclient.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.common.callback.Callback;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;


public class RequestBasedMetadataTestUtils {
  private static final int CURRENT_VERSION = 1;
  public static final String REPLICA1_NAME = "host1";
  public static final String REPLICA2_NAME = "host2";
  public static final String NEW_REPLICA_NAME = "host3";
  public static final String KEY_SCHEMA = "\"string\"";
  public static final String VALUE_SCHEMA = "\"string\"";
  public static final String SERVER_D2_SERVICE = "test-d2-service";
  private static final byte[] DICTIONARY = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();

  public static ClientConfig getMockClientConfig(String storeName) {
    return getMockClientConfig(storeName, false, true);
  }

  public static ClientConfig getMockClientConfig(
      String storeName,
      boolean firstConnWarmupFails,
      boolean isMetadataConnWarmupEnabled) {
    ClientConfig clientConfig = mock(ClientConfig.class);
    ClusterStats clusterStats = new ClusterStats(new MetricsRepository(), storeName);
    doReturn(getMockR2Client(firstConnWarmupFails)).when(clientConfig).getR2Client();
    doReturn(1L).when(clientConfig).getMetadataRefreshIntervalInSeconds();
    doReturn(1L).when(clientConfig).getMetadataConnWarmupTimeoutInSeconds();
    doReturn(isMetadataConnWarmupEnabled).when(clientConfig).isMetadataConnWarmupEnabled();
    doReturn(storeName).when(clientConfig).getStoreName();
    doReturn(clusterStats).when(clientConfig).getClusterStats();
    doReturn(ClientRoutingStrategyType.LEAST_LOADED).when(clientConfig).getClientRoutingStrategyType();
    doReturn(mock(FastClientStats.class)).when(clientConfig).getStats(RequestType.SINGLE_GET);
    return clientConfig;
  }

  /**
   * This r2Client is used for warmup conns to instances as part of metadata update
   */
  public static Client getMockR2Client(boolean firstConnWarmupFails) {
    Client r2Client = mock(Client.class);
    if (!firstConnWarmupFails) {
      doAnswer(invocation -> {
        R2TransportClient.R2TransportClientCallback callback = invocation.getArgument(1);
        callback.getValueFuture().complete(null);
        return null;
      }).when(r2Client)
          .restRequest(
              argThat(
                  argument -> argument.getURI().getPath().endsWith("/" + QueryAction.HEALTH.toString().toLowerCase())),
              any(Callback.class));
    } else {
      // for REPLICA1_NAME: first request fails and other succeeds
      doAnswer(invocation -> {
        R2TransportClient.R2TransportClientCallback callback = invocation.getArgument(1);
        callback.getValueFuture().completeExceptionally(new VeniceClientHttpException(500));
        return null;
      }).doAnswer(invocation -> {
        R2TransportClient.R2TransportClientCallback callback = invocation.getArgument(1);
        callback.getValueFuture().complete(null);
        return null;
      })
          .when(r2Client)
          .restRequest(
              argThat(
                  argument -> (argument.getURI().getPath().endsWith("/" + QueryAction.HEALTH.toString().toLowerCase())
                      && argument.getURI().getPath().contains(REPLICA1_NAME))),
              any(Callback.class));

      // for other replicas: all request succeeds
      doAnswer(invocation -> {
        R2TransportClient.R2TransportClientCallback callback = invocation.getArgument(1);
        callback.getValueFuture().complete(null);
        return null;
      }).when(r2Client)
          .restRequest(
              argThat(
                  argument -> (argument.getURI().getPath().endsWith("/" + QueryAction.HEALTH.toString().toLowerCase())
                      && !argument.getURI().getPath().contains(REPLICA1_NAME))),
              any(Callback.class));
    }
    return r2Client;
  }

  public static D2TransportClient getMockD2TransportClient(
      String storeName,
      boolean changeMetadata,
      Schema storeKeySchema,
      Schema storeValueSchema) {
    D2TransportClient d2TransportClient = mock(D2TransportClient.class);

    Map<String, String> partitionerParams = new HashMap<>();
    partitionerParams.put("testKey", "testValue");

    VersionProperties versionProperties = new VersionProperties(
        CURRENT_VERSION,
        CompressionStrategy.ZSTD_WITH_DICT.getValue(),
        2,
        "com.linkedin.venice.partitioner.DefaultVenicePartitioner",
        Collections.unmodifiableMap(partitionerParams),
        1);
    Map<CharSequence, List<CharSequence>> routeMap = new HashMap<>();
    routeMap.put("0", Collections.singletonList(REPLICA1_NAME));
    routeMap.put("1", Collections.singletonList(REPLICA2_NAME));
    Map<CharSequence, Integer> helixGroupMap = new HashMap<>();
    helixGroupMap.put(REPLICA1_NAME, 0);
    helixGroupMap.put(REPLICA2_NAME, 1);
    MetadataResponseRecord metadataResponse = new MetadataResponseRecord(
        versionProperties,
        Collections.singletonList(CURRENT_VERSION),
        Collections.singletonMap("1", storeKeySchema.toString()),
        Collections.singletonMap("1", storeValueSchema.toString()),
        1,
        routeMap,
        helixGroupMap,
        150);

    byte[] metadataBody = SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$)
        .serialize(metadataResponse);
    routeMap.put("0", Collections.singletonList(NEW_REPLICA_NAME));
    metadataResponse.setRoutingInfo(routeMap);
    byte[] newMetadataBody = SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$)
        .serialize(metadataResponse);
    int metadataResponseSchemaId = AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion();
    TransportClientResponse transportClientMetadataResponse =
        new TransportClientResponse(metadataResponseSchemaId, CompressionStrategy.NO_OP, metadataBody);
    CompletableFuture<TransportClientResponse> completableMetadataFuture =
        CompletableFuture.completedFuture(transportClientMetadataResponse);
    CompletableFuture<TransportClientResponse> completableMetadataFuture2 = CompletableFuture.completedFuture(
        new TransportClientResponse(metadataResponseSchemaId + 1, CompressionStrategy.NO_OP, newMetadataBody));

    TransportClientResponse transportClientDictionaryResponse =
        new TransportClientResponse(0, CompressionStrategy.NO_OP, DICTIONARY);
    CompletableFuture<TransportClientResponse> completableDictionaryFuture =
        CompletableFuture.completedFuture(transportClientDictionaryResponse);

    if (changeMetadata) {
      when(d2TransportClient.get(eq(QueryAction.METADATA.toString().toLowerCase() + "/" + storeName)))
          .thenReturn(completableMetadataFuture, completableMetadataFuture2);
    } else {
      when(d2TransportClient.get(eq(QueryAction.METADATA.toString().toLowerCase() + "/" + storeName)))
          .thenReturn(completableMetadataFuture);
    }
    doReturn(completableDictionaryFuture).when(d2TransportClient)
        .get(eq(QueryAction.DICTIONARY.toString().toLowerCase() + "/" + storeName + "/" + CURRENT_VERSION));

    return d2TransportClient;
  }

  public static D2ServiceDiscovery getMockD2ServiceDiscovery(D2TransportClient d2TransportClient, String storeName) {
    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);
    D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    d2ServiceDiscoveryResponse.setServerD2Service(SERVER_D2_SERVICE);

    doReturn(d2ServiceDiscoveryResponse).when(d2ServiceDiscovery)
        .find(eq(d2TransportClient), eq(storeName), anyBoolean());

    return d2ServiceDiscovery;
  }

  public static VeniceCompressor getZstdVeniceCompressor(String storeName) {
    String resourceName = storeName + "_v" + CURRENT_VERSION;

    return new CompressorFactory()
        .createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, resourceName, DICTIONARY);
  }

  public static RouterBackedSchemaReader getMockRouterBackedSchemaReader() {
    RouterBackedSchemaReader metadataResponseSchemaReader = mock(RouterBackedSchemaReader.class);
    int latestSchemaId = AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion();
    when(metadataResponseSchemaReader.getLatestValueSchemaId()).thenReturn(latestSchemaId, latestSchemaId + 1);
    doReturn(MetadataResponseRecord.SCHEMA$).when(metadataResponseSchemaReader).getValueSchema(latestSchemaId);
    doReturn(MetadataResponseRecord.SCHEMA$).when(metadataResponseSchemaReader).getValueSchema(latestSchemaId + 1);
    return metadataResponseSchemaReader;
  }

  public static RequestBasedMetadata getMockMetaData(ClientConfig clientConfig, String storeName)
      throws InterruptedException {
    return getMockMetaData(clientConfig, storeName, getMockRouterBackedSchemaReader(), false, false, false, null);
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      boolean metadataChange) throws InterruptedException {
    return getMockMetaData(
        clientConfig,
        storeName,
        getMockRouterBackedSchemaReader(),
        metadataChange,
        false,
        false,
        null);
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      RouterBackedSchemaReader routerBackedSchemaReader,
      boolean metadataChange) throws InterruptedException {
    return getMockMetaData(clientConfig, storeName, routerBackedSchemaReader, metadataChange, false, false, null);
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      boolean metadataChange,
      boolean mockMetadataUpdateFailure,
      boolean firstUpdateFails,
      ScheduledExecutorService scheduler) throws InterruptedException {
    return getMockMetaData(
        clientConfig,
        storeName,
        getMockRouterBackedSchemaReader(),
        metadataChange,
        mockMetadataUpdateFailure,
        firstUpdateFails,
        scheduler);
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      RouterBackedSchemaReader routerBackedSchemaReader,
      boolean metadataChange,
      boolean mockMetadataUpdateFailure,
      boolean firstUpdateFails,
      ScheduledExecutorService scheduler) throws InterruptedException {
    return getMockMetaData(
        clientConfig,
        storeName,
        routerBackedSchemaReader,
        metadataChange,
        mockMetadataUpdateFailure,
        firstUpdateFails,
        scheduler,
        AvroCompatibilityHelper.parse(KEY_SCHEMA),
        AvroCompatibilityHelper.parse(VALUE_SCHEMA));
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      RouterBackedSchemaReader routerBackedSchemaReader,
      boolean metadataChange,
      boolean mockMetadataUpdateFailure,
      boolean firstUpdateFails,
      ScheduledExecutorService scheduler,
      Schema storeKeySchema,
      Schema storeValueSchema) throws InterruptedException {
    D2TransportClient d2TransportClient =
        getMockD2TransportClient(storeName, metadataChange, storeKeySchema, storeValueSchema);
    D2ServiceDiscovery d2ServiceDiscovery = getMockD2ServiceDiscovery(d2TransportClient, storeName);
    RequestBasedMetadata requestBasedMetadata;
    if (mockMetadataUpdateFailure) {
      requestBasedMetadata = mock(RequestBasedMetadata.class);
      doAnswer(invocation -> null).when(requestBasedMetadata).discoverD2Service();

      if (firstUpdateFails) {
        doAnswer(invocation -> {
          throw new VeniceClientException("update cache exception");
        }).doAnswer(invocation -> null).when(requestBasedMetadata).updateCache(anyBoolean());
      } else {
        doAnswer(invocation -> null).when(requestBasedMetadata).updateCache(anyBoolean());
      }

      doCallRealMethod().when(requestBasedMetadata).setIsReadyLatch(any());
      doCallRealMethod().when(requestBasedMetadata).getIsReadyLatch();
      doCallRealMethod().when(requestBasedMetadata).setScheduler(any());
      doCallRealMethod().when(requestBasedMetadata).getScheduler();
      doCallRealMethod().when(requestBasedMetadata).setRefreshIntervalInSeconds(anyLong());
      doCallRealMethod().when(requestBasedMetadata).getRefreshIntervalInSeconds();
      requestBasedMetadata.setIsReadyLatch(new CountDownLatch(1));
      ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
      requestBasedMetadata.setScheduler(mockScheduler);
      requestBasedMetadata.setRefreshIntervalInSeconds(RequestBasedMetadata.DEFAULT_REFRESH_INTERVAL_IN_SECONDS);
      doAnswer(invocation -> {
        scheduler.schedule((Runnable) invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
        return null;
      }).when(mockScheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
      doCallRealMethod().when(requestBasedMetadata).start();
    } else {
      requestBasedMetadata = new RequestBasedMetadata(clientConfig, d2TransportClient);
    }
    requestBasedMetadata.setMetadataResponseSchemaReader(routerBackedSchemaReader);
    requestBasedMetadata.setD2ServiceDiscovery(d2ServiceDiscovery);
    return requestBasedMetadata;
  }
}
