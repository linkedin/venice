package com.linkedin.venice.blobtransfer;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;


/**
 * Discovers the Venice servers hosting a store partition so a Stateful CDC / Da Vinci client with no peer can fall back
 * to fetching a blob from a server. Used only on the cold-start fallback path.
 *
 * <p>{@code metadata/<store>} is served by Venice servers, not the Router, so this finder uses D2 server-routing (like
 * Fast Client's {@code RequestBasedMetadata}): it resolves the store's server D2 service, GETs {@code metadata/<store>}
 * on a server, and reads the partition-to-replica map from {@link MetadataResponseRecord#getRoutingInfo()}, reusing the
 * D2 client on the {@link ClientConfig}. The response is decoded against the writer schema the server advertises (via
 * its schema id), so a server running a newer {@link MetadataResponseRecord} schema than the client is still readable.
 * Any failure returns an empty/error response so the caller falls back to Version Topic replay -- it never returns a
 * wrong host.
 */
public class MetadataBasedServerBlobFinder implements BlobFinder {
  private static final Logger LOGGER = LogManager.getLogger(MetadataBasedServerBlobFinder.class);
  /** Short timeout so a slow or unreachable server fails fast to Version Topic replay rather than stalling. */
  private static final int METADATA_FETCH_TIMEOUT_SECONDS = 3;

  /** Fallback deserializer using the client's compiled schema, for responses that carry no usable writer schema id. */
  private static final RecordDeserializer<MetadataResponseRecord> COMPILED_METADATA_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(MetadataResponseRecord.SCHEMA$, MetadataResponseRecord.class);

  private final ClientConfig clientConfig;
  private final D2ServiceDiscovery d2ServiceDiscovery;

  /**
   * One transport per store, cached after discovery points it at the store's server D2 service: avoids re-resolving on
   * each cold start and keeps {@code setServiceName} from racing across stores.
   */
  private final VeniceConcurrentHashMap<String, D2TransportClient> storeToServerTransportClientMap =
      new VeniceConcurrentHashMap<>();

  /**
   * Resolves a {@link MetadataResponseRecord} writer schema from the server-advertised schema id; built lazily on first
   * use (or injected for tests). Reuses the config's D2 client to read the metadata-response system store's schemas.
   */
  private volatile RouterBackedSchemaReader metadataResponseSchemaReader;

  public MetadataBasedServerBlobFinder(ClientConfig clientConfig) {
    this(clientConfig, new D2ServiceDiscovery(), null);
  }

  @VisibleForTesting
  MetadataBasedServerBlobFinder(ClientConfig clientConfig, D2ServiceDiscovery d2ServiceDiscovery) {
    this(clientConfig, d2ServiceDiscovery, null);
  }

  @VisibleForTesting
  MetadataBasedServerBlobFinder(
      ClientConfig clientConfig,
      D2ServiceDiscovery d2ServiceDiscovery,
      RouterBackedSchemaReader metadataResponseSchemaReader) {
    this.clientConfig = clientConfig;
    this.d2ServiceDiscovery = d2ServiceDiscovery;
    this.metadataResponseSchemaReader = metadataResponseSchemaReader;
  }

  /**
   * Lazily build and cache one transport per store, routed to that store's server D2 service via D2 discovery on the
   * config's D2 client. Closed in {@link #close()}.
   *
   * @throws VeniceClientException if the config has no D2 client (caller falls back to Version Topic replay).
   */
  @VisibleForTesting
  D2TransportClient getServerTransportClient(String storeName) {
    return storeToServerTransportClientMap.computeIfAbsent(storeName, k -> {
      D2TransportClient transportClient = getD2TransportClient(storeName);
      try {
        D2ServiceDiscoveryResponse discoveryResponse = d2ServiceDiscovery.find(transportClient, storeName, true);
        String serverD2ServiceName = discoveryResponse == null ? null : discoveryResponse.getServerD2Service();
        if (serverD2ServiceName == null || serverD2ServiceName.trim().isEmpty()) {
          throw new VeniceClientException("No server D2 service discovered for store: " + storeName);
        }
        transportClient.setServiceName(serverD2ServiceName);
        LOGGER.info(
            "Resolved server D2 service {} for server blob discovery of store: {}",
            serverD2ServiceName,
            storeName);
        return transportClient;
      } catch (RuntimeException e) {
        transportClient.close();
        throw e;
      }
    });
  }

  private @NonNull D2TransportClient getD2TransportClient(String storeName) {
    D2Client d2Client = clientConfig.getD2Client();
    if (d2Client == null) {
      throw new VeniceClientException(
          "Server blob discovery requires a D2 client on the client config, but none was configured for store: "
              + storeName);
    }

    return new D2TransportClient(clientConfig.getD2ServiceName(), d2Client);
  }

  /**
   * Return the server replicas hosting {@code partitionId} for the store's current serving version ({@code version} is
   * only for error reporting). Any failure returns an error response so the caller falls back to Version Topic replay.
   */
  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partitionId) {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    try {
      String uri = QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
      LOGGER.debug("Fetching {} to discover servers hosting store {} partition {}.", uri, storeName, partitionId);
      TransportClientResponse metadataResponse =
          getServerTransportClient(storeName).get(uri).get(METADATA_FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (metadataResponse == null || metadataResponse.getBody() == null) {
        return errorResponse(storeName, version, partitionId, "metadata response body was null");
      }
      MetadataResponseRecord metadata = deserializeMetadata(metadataResponse);
      if (metadata.getVersionMetadata() == null || metadata.getVersionMetadata().getCurrentVersion() != version) {
        return errorResponse(
            storeName,
            version,
            partitionId,
            "metadata current version was " + (metadata.getVersionMetadata() == null
                ? "missing"
                : String.valueOf(metadata.getVersionMetadata().getCurrentVersion())));
      }
      List<String> replicas = extractReplicas(metadata.getRoutingInfo(), partitionId);
      LOGGER.debug(
          "Discovered {} server replica(s) for store {} partition {}: {}",
          replicas.size(),
          storeName,
          partitionId,
          replicas);
      response.setDiscoveryResult(replicas);
      return response;
    } catch (Exception e) {
      return errorResponse(storeName, version, partitionId, e);
    }
  }

  /**
   * Decode the metadata response against the writer schema the server advertises (its schema id), so a server running a
   * newer {@link MetadataResponseRecord} schema than this client is still readable. Falls back to the client's compiled
   * schema when the response carries no usable schema id or the writer schema cannot be resolved.
   */
  private MetadataResponseRecord deserializeMetadata(TransportClientResponse metadataResponse) {
    byte[] body = metadataResponse.getBody();
    if (metadataResponse.isSchemaIdValid()) {
      try {
        Schema writerSchema = getMetadataResponseSchemaReader().getValueSchema(metadataResponse.getSchemaId());
        if (writerSchema != null) {
          return FastSerializerDeserializerFactory
              .getFastAvroSpecificDeserializer(writerSchema, MetadataResponseRecord.class)
              .deserialize(body);
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Could not resolve the metadata response writer schema for id {}; decoding with the compiled schema.",
            metadataResponse.getSchemaId(),
            e);
      }
    }
    return COMPILED_METADATA_DESERIALIZER.deserialize(body);
  }

  private RouterBackedSchemaReader getMetadataResponseSchemaReader() {
    RouterBackedSchemaReader reader = metadataResponseSchemaReader;
    if (reader == null) {
      synchronized (this) {
        reader = metadataResponseSchemaReader;
        if (reader == null) {
          reader = buildMetadataResponseSchemaReader();
          metadataResponseSchemaReader = reader;
        }
      }
    }
    return reader;
  }

  /**
   * Build a schema reader for the metadata-response system store so a server-advertised writer-schema id can be
   * resolved to its schema. Mirrors Fast Client's {@code RequestBasedMetadata}, reusing the config's D2 client and
   * cluster-discovery service.
   *
   * @throws VeniceClientException if the config has no D2 client (the caller then decodes with the compiled schema).
   */
  private RouterBackedSchemaReader buildMetadataResponseSchemaReader() {
    D2Client d2Client = clientConfig.getD2Client();
    if (d2Client == null) {
      throw new VeniceClientException("Server blob discovery requires a D2 client to resolve the metadata schema.");
    }
    InternalAvroStoreClient schemaStoreClient = new AvroGenericStoreClientImpl(
        new D2TransportClient(clientConfig.getD2ServiceName(), d2Client),
        false,
        ClientConfig.defaultGenericClientConfig(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName()));
    return new RouterBackedSchemaReader(() -> schemaStoreClient, Optional.empty(), Optional.empty());
  }

  /**
   * Extract the replica hosts for {@code partitionId} from the routing map (keyed by partition number as a string);
   * empty if the partition is absent. Raw-{@link Map}-typed so it is unit-testable without Avro.
   */
  @VisibleForTesting
  static List<String> extractReplicas(Map<?, ?> routingInfo, int partitionId) {
    List<String> replicas = new ArrayList<>();
    if (routingInfo == null) {
      return replicas;
    }
    String targetKey = String.valueOf(partitionId);
    for (Map.Entry<?, ?> entry: routingInfo.entrySet()) {
      if (entry.getKey() != null && targetKey.equals(entry.getKey().toString()) && entry.getValue() instanceof List) {
        for (Object replica: (List<?>) entry.getValue()) {
          String host = normalizeHost(replica == null ? null : replica.toString());
          if (host != null && !host.isEmpty()) {
            replicas.add(host);
          }
        }
        break;
      }
    }
    return replicas;
  }

  /**
   * Reduce a routing entry to its bare host: server metadata values are instance URLs ({@code https://host:port}), but
   * the blob client connects by host on its own p2p port. Host-only entries pass through; IPv6 literals are unbracketed.
   */
  @VisibleForTesting
  static String normalizeHost(String replica) {
    if (replica == null) {
      return null;
    }
    String value = replica.trim();
    if (value.isEmpty()) {
      return null;
    }
    if (value.contains("://")) {
      try {
        String parsedHost = URI.create(value).getHost();
        if (parsedHost != null && !parsedHost.isEmpty()) {
          value = parsedHost;
        }
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Could not parse host from metadata replica entry: {}", replica);
      }
    }
    if (value.length() > 1 && value.startsWith("[") && value.endsWith("]")) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  private BlobPeersDiscoveryResponse errorResponse(String storeName, int version, int partitionId, String message) {
    BlobPeersDiscoveryResponse response = buildErrorResponse(storeName, version, partitionId, message);
    LOGGER.warn(response.getErrorMessage());
    return response;
  }

  private BlobPeersDiscoveryResponse errorResponse(String storeName, int version, int partitionId, Exception e) {
    BlobPeersDiscoveryResponse response =
        buildErrorResponse(storeName, version, partitionId, e.getMessage() == null ? e.toString() : e.getMessage());
    LOGGER.warn(response.getErrorMessage(), e);
    return response;
  }

  private BlobPeersDiscoveryResponse buildErrorResponse(
      String storeName,
      int version,
      int partitionId,
      String message) {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setError(true);
    response.setErrorMessage(
        String.format(
            "Error finding servers for blob transfer of store: %s, version: %d, partition: %d. Error: %s",
            storeName,
            version,
            partitionId,
            message));
    return response;
  }

  @Override
  public void close() {
    for (D2TransportClient transportClient: storeToServerTransportClientMap.values()) {
      // Releases this transport only; the shared D2 client (owned by the config) keeps running.
      transportClient.close();
    }
    RouterBackedSchemaReader reader = metadataResponseSchemaReader;
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing the metadata response schema reader.", e);
      }
    }
  }
}
