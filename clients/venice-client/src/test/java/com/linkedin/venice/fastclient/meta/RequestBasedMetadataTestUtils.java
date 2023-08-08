package com.linkedin.venice.fastclient.meta;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
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
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;


public class RequestBasedMetadataTestUtils {
  private static final int CURRENT_VERSION = 1;
  public static final String REPLICA_NAME = "host1";
  public static final String NEW_REPLICA_NAME = "host2";
  public static final String KEY_SCHEMA = "\"string\"";
  public static final String VALUE_SCHEMA = "\"string\"";
  private static final byte[] DICTIONARY = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();

  public static ClientConfig getMockClientConfig(String storeName) {
    ClientConfig clientConfig = mock(ClientConfig.class);
    ClusterStats clusterStats = new ClusterStats(new MetricsRepository(), storeName);
    doReturn(1L).when(clientConfig).getMetadataRefreshIntervalInSeconds();
    doReturn(storeName).when(clientConfig).getStoreName();
    doReturn(clusterStats).when(clientConfig).getClusterStats();
    doReturn(ClientRoutingStrategyType.LEAST_LOADED).when(clientConfig).getClientRoutingStrategyType();
    doReturn(mock(FastClientStats.class)).when(clientConfig).getStats(RequestType.SINGLE_GET);
    doReturn(mock(InternalAvroStoreClient.class)).when(clientConfig).getMetadataResponseSchemaStoreClient();
    return clientConfig;
  }

  public static D2TransportClient getMockD2TransportClient(String storeName) {
    D2TransportClient d2TransportClient = mock(D2TransportClient.class);

    VersionProperties versionProperties = new VersionProperties(
        CURRENT_VERSION,
        CompressionStrategy.ZSTD_WITH_DICT.getValue(),
        1,
        "com.linkedin.venice.partitioner.DefaultVenicePartitioner",
        Collections.emptyMap(),
        1);
    MetadataResponseRecord metadataResponse = new MetadataResponseRecord(
        versionProperties,
        Collections.singletonList(CURRENT_VERSION),
        Collections.singletonMap("1", KEY_SCHEMA),
        Collections.singletonMap("1", VALUE_SCHEMA),
        1,
        Collections.singletonMap("0", Collections.singletonList(REPLICA_NAME)),
        Collections.singletonMap(REPLICA_NAME, 0));

    byte[] metadataBody = SerializerDeserializerFactory.getAvroGenericSerializer(MetadataResponseRecord.SCHEMA$)
        .serialize(metadataResponse);
    metadataResponse.setRoutingInfo(Collections.singletonMap("0", Collections.singletonList(NEW_REPLICA_NAME)));
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

    when(d2TransportClient.get(eq(QueryAction.METADATA.toString().toLowerCase() + "/" + storeName)))
        .thenReturn(completableMetadataFuture, completableMetadataFuture2);
    doReturn(completableDictionaryFuture).when(d2TransportClient)
        .get(eq(QueryAction.DICTIONARY.toString().toLowerCase() + "/" + storeName + "/" + CURRENT_VERSION));

    return d2TransportClient;
  }

  public static D2ServiceDiscovery getMockD2ServiceDiscovery(D2TransportClient d2TransportClient, String storeName) {
    D2ServiceDiscovery d2ServiceDiscovery = mock(D2ServiceDiscovery.class);

    D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse = new D2ServiceDiscoveryResponse();

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

  public static RequestBasedMetadata getMockMetaData(ClientConfig clientConfig, String storeName) {
    return getMockMetaData(clientConfig, storeName, getMockRouterBackedSchemaReader());
  }

  public static RequestBasedMetadata getMockMetaData(
      ClientConfig clientConfig,
      String storeName,
      RouterBackedSchemaReader routerBackedSchemaReader) {
    D2TransportClient d2TransportClient = RequestBasedMetadataTestUtils.getMockD2TransportClient(storeName);
    D2ServiceDiscovery d2ServiceDiscovery =
        RequestBasedMetadataTestUtils.getMockD2ServiceDiscovery(d2TransportClient, storeName);
    RequestBasedMetadata requestBasedMetadata = new RequestBasedMetadata(clientConfig, d2TransportClient);
    requestBasedMetadata.setMetadataResponseSchemaReader(routerBackedSchemaReader);
    requestBasedMetadata.setD2ServiceDiscovery(d2ServiceDiscovery);
    requestBasedMetadata.start();
    return requestBasedMetadata;
  }
}
