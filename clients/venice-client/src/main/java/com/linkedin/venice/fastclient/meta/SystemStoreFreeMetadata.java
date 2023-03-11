package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.MissingKeyInStoreMetadataException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SystemStoreFreeMetadata extends AbstractStoreMetadata {
  private static final Logger LOGGER = LogManager.getLogger(SystemStoreFreeMetadata.class);
  private static final String VERSION_PARTITION_SEPARATOR = "_";
  private static final long ZSTD_DICT_FETCH_TIMEOUT = 10;
  private static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final long INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS = 30;
  private static final long RETRY_WAIT_TIME_IN_MS = 1000;
  private final long refreshIntervalInSeconds;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final AtomicInteger currentVersion = new AtomicInteger();
  private final AtomicInteger latestSuperSetValueSchemaId = new AtomicInteger();
  private final AtomicReference<SchemaData> schemas = new AtomicReference<>();
  private final Map<String, List<String>> readyToServeInstancesMap = new VeniceConcurrentHashMap<>();
  private final Map<Integer, Pair<VenicePartitioner, Integer>> versionPartitionerMap = new VeniceConcurrentHashMap<>();
  private int partitionCount;
  private final Map<Integer, ByteBuffer> versionZstdDictionaryMap = new VeniceConcurrentHashMap<>();
  private final CompressorFactory compressorFactory;
  private final TransportClient r2TransportClient;
  private final TransportClient d2TransportClient;
  private final ClusterStats clusterStats;
  private volatile boolean isServiceDiscovered;
  private String serverD2ServiceName;

  public SystemStoreFreeMetadata(ClientConfig clientConfig, TransportClient d2TransportClient) {
    super(clientConfig);
    this.refreshIntervalInSeconds = clientConfig.getMetadataRefreshIntervalInSeconds() > 0
        ? clientConfig.getMetadataRefreshIntervalInSeconds()
        : DEFAULT_REFRESH_INTERVAL_IN_SECONDS;
    this.r2TransportClient = new R2TransportClient(clientConfig.getR2Client());
    this.d2TransportClient = d2TransportClient;
    this.compressorFactory = new CompressorFactory();
    this.clusterStats = clientConfig.getClusterStats();
  }

  @Override
  public int getCurrentStoreVersion() {
    return currentVersion.get();
  }

  @Override
  public int getPartitionId(int version, ByteBuffer key) {
    Pair<VenicePartitioner, Integer> partitionerPair = versionPartitionerMap.get(version);
    if (partitionerPair == null) {
      throw new VeniceClientException("Unknown version number: " + version + " for store: " + storeName);
    }
    return partitionerPair.getFirst().getPartitionId(key, partitionerPair.getSecond());
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    String key = getVersionPartitionMapKey(version, partitionId);
    return readyToServeInstancesMap.getOrDefault(key, Collections.emptyList());
  }

  private String getVersionPartitionMapKey(int version, int partition) {
    return version + VERSION_PARTITION_SEPARATOR + partition;
  }

  @Override
  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      String resourceName = getResourceName(version);
      VeniceCompressor compressor = compressorFactory.getVersionSpecificCompressor(resourceName);
      if (compressor == null) {
        ByteBuffer dictionary = versionZstdDictionaryMap.get(version);
        if (dictionary == null) {
          throw new VeniceClientException(
              String.format(
                  "No dictionary available for decompressing zstd payload for store %s version %d ",
                  storeName,
                  version));
        } else {
          compressor = compressorFactory
              .createVersionSpecificCompressorIfNotExist(compressionStrategy, resourceName, dictionary.array());
        }
      }
      return compressor;
    } else {
      return compressorFactory.getCompressor(compressionStrategy);
    }
  }

  private String getResourceName(int version) {
    return storeName + "_v" + version;
  }

  @Override
  public void start() {
    // perform cluster discovery work upfront and retrieve the server d2 service name
    discoverD2Service(false);

    // build a base for future metadata updates then start periodic refresh
    long timeoutTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(INITIAL_UPDATE_CACHE_TIMEOUT_IN_SECONDS);
    while (true) {
      try {
        updateCache(false);
        break;
      } catch (MissingKeyInStoreMetadataException e) {
        if (System.currentTimeMillis() > timeoutTime) {
          throw e;
        }
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void discoverD2Service(boolean retryOnFailure) {
    if (isServiceDiscovered) {
      return;
    }
    synchronized (this) {
      if (isServiceDiscovered) {
        return;
      }
      if (d2TransportClient instanceof D2TransportClient) {
        D2TransportClient client = (D2TransportClient) d2TransportClient;
        serverD2ServiceName = new D2ServiceDiscovery().find(client, storeName, retryOnFailure).getRouterD2Service();
        // TODO: do we ever use the d2Client to make requests? AbstractAvroStoreClient uses it...
        client.setServiceName(serverD2ServiceName);
      }
      isServiceDiscovered = true;
    }
  }

  private synchronized void updateCache(boolean onDemandRefresh) {
    // call the METADATA endpoint
    try {
      byte[] body = fetchMetadata().get().getBody();
      RecordDeserializer<MetadataResponseRecord> metadataResponseRecordRecordDeserializer =
          SerializerDeserializerFactory.getAvroGenericDeserializer(MetadataResponseRecord.SCHEMA$);
      GenericRecord metadataResponse = metadataResponseRecordRecordDeserializer.deserialize(body);
      GenericRecord versionMetadata = (GenericRecord) metadataResponse.get("versionMetadata");

      Integer fetchedVersion = (Integer) versionMetadata.get("currentVersion");
      Integer compressionStrategy = (Integer) versionMetadata.get("compressionStrategy");
      Integer newPartitionCount = (Integer) versionMetadata.get("partitionCount");
      String partitionerClass = ((Utf8) versionMetadata.get("partitionerClass")).toString();
      Integer newSuperSetValueSchemaId = (Integer) versionMetadata.get("latestSuperSetValueSchemaId");
      Map<String, String> partitionerParams =
          ((HashMap<Utf8, Utf8>) versionMetadata.get("partitionerParams")).entrySet()
              .stream()
              .collect(Collectors.toMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString()));
      Integer amplificationFactor = (Integer) versionMetadata.get("amplificationFactor");
      Map<Integer, String> keySchema = ((HashMap<Utf8, Utf8>) metadataResponse.get("keySchema")).entrySet()
          .stream()
          .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey().toString()), e -> e.getValue().toString()));
      Map<Integer, String> valueSchemas = ((HashMap<Utf8, Utf8>) metadataResponse.get("valueSchemas")).entrySet()
          .stream()
          .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey().toString()), e -> e.getValue().toString()));
      Map<Utf8, Integer> helixGroupInfo = (HashMap<Utf8, Integer>) metadataResponse.get("helixGroupInfo");
      Map<Integer, List<String>> routingInfo =
          ((HashMap<Utf8, Collection<Utf8>>) metadataResponse.get("routingInfo")).entrySet()
              .stream()
              .collect(
                  Collectors.toMap(
                      e -> Integer.valueOf(e.getKey().toString()),
                      e -> e.getValue().stream().map(Utf8::toString).collect(Collectors.toList())));

      if (fetchedVersion != getCurrentStoreVersion()) {
        // call the DICTIONARY endpoint if needed
        CompletableFuture<TransportClientResponse> dictionaryFetchFuture = null;
        if (!versionZstdDictionaryMap.containsKey(fetchedVersion)
            && compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT.getValue()) {
          dictionaryFetchFuture = fetchCompressionDictionary(fetchedVersion);
        }

        // Update partitioner pair map (versionPartitionerMap)
        versionPartitionerMap.computeIfAbsent(fetchedVersion, k -> {
          Properties params = new Properties();
          params.putAll(partitionerParams);
          VenicePartitioner partitioner =
              PartitionUtils.getVenicePartitioner(partitionerClass, amplificationFactor, new VeniceProperties(params));
          return new Pair<>(partitioner, newPartitionCount);
        });

        // Update readyToServeInstanceMap
        for (int i = 0; i < newPartitionCount; i++) {
          final int partitionId = i;
          String key = getVersionPartitionMapKey(fetchedVersion, partitionId);
          readyToServeInstancesMap.compute(key, (k, v) -> routingInfo.get(partitionId));
        }

        // Update schemas
        SchemaData schemaData = new SchemaData(storeName);
        for (Map.Entry<Integer, String> entry: keySchema.entrySet()) {
          schemaData.setKeySchema(new SchemaEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<Integer, String> entry: valueSchemas.entrySet()) {
          schemaData.addValueSchema(new SchemaEntry(entry.getKey(), entry.getValue()));
        }
        schemas.set(schemaData);

        // Evict old entries
        versionPartitionerMap.remove(currentVersion.get());
        versionZstdDictionaryMap.remove(currentVersion.get());
        for (int i = 0; i < partitionCount; i++) {
          readyToServeInstancesMap.remove(getVersionPartitionMapKey(currentVersion.get(), i));
        }
        partitionCount = newPartitionCount;

        // Wait for dictionary fetch to finish if there is one
        try {
          if (dictionaryFetchFuture != null) {
            dictionaryFetchFuture.get(ZSTD_DICT_FETCH_TIMEOUT, TimeUnit.SECONDS);
          }
          currentVersion.set(fetchedVersion);
          clusterStats.updateCurrentVersion(getCurrentStoreVersion());
          latestSuperSetValueSchemaId.set(newSuperSetValueSchemaId);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          throw new VeniceClientException("Dictionary fetch operation was interrupted");
        } catch (ExecutionException | TimeoutException e) {
          LOGGER.warn(
              "Dictionary fetch operation could not complete in time for some of the versions. "
                  + "Will be retried on next refresh",
              e);
          clusterStats.recordVersionUpdateFailure();
        }

      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new VeniceClientException("Metadata fetch operation was interrupted");
    } catch (ExecutionException e) {
      // perform an on demand refresh if update fails in case of store migration, otherwise propagate the error
      if (!onDemandRefresh) {
        updateCache(true);
      } else {
        throw new VeniceClientException("Metadata fetch operation has failed");
      }
    }

    // update the refresh interval to a random interval and queue the next refresh if this wasn't an on demand refresh
    if (!onDemandRefresh) {
      long randomRefreshInterval = refreshIntervalInSeconds + ThreadLocalRandom.current().nextInt(-10, 10);
      scheduler.schedule(this::refresh, randomRefreshInterval, TimeUnit.SECONDS);
    }
  }

  private void refresh() {
    try {
      updateCache(false);
    } catch (Exception e) {
      // Catch all errors so periodic refresh doesn't break on transient errors.
      LOGGER.error("Encountered unexpected error during periodic refresh", e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    readyToServeInstancesMap.clear();
    versionPartitionerMap.clear();
    partitionCount = 0;
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  private CompletableFuture<TransportClientResponse> fetchMetadata() {
    CompletableFuture<TransportClientResponse> metadataFuture = new CompletableFuture<>();
    String url = serverD2ServiceName + "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;

    LOGGER.info("Fetching metadata for store {} from URL {} ", storeName, url);
    r2TransportClient.get(url).whenComplete((response, throwable) -> {
      if (throwable != null) {
        String message = String.format("Problem fetching metadata from URL:%s for store:%s ", url, storeName);
        LOGGER.warn(message, throwable);
        metadataFuture.completeExceptionally(throwable);
      } else {
        metadataFuture.complete(response);
      }
    });
    return metadataFuture;
  }

  private CompletableFuture<TransportClientResponse> fetchCompressionDictionary(int version) {
    CompletableFuture<TransportClientResponse> compressionDictionaryFuture = new CompletableFuture<>();
    String url =
        serverD2ServiceName + "/" + QueryAction.DICTIONARY.toString().toLowerCase() + "/" + storeName + "/" + version;

    LOGGER.info("Fetching compression dictionary for version {} from URL {} ", version, url);
    r2TransportClient.get(url).whenComplete((response, throwable) -> {
      if (throwable != null) {
        String message = String.format(
            "Problem fetching zstd compression dictionary from URL:%s for store:%s , version:%d",
            url,
            storeName,
            version);
        LOGGER.warn(message, throwable);
        compressionDictionaryFuture.completeExceptionally(throwable);
      } else {
        byte[] dictionary = response.getBody();
        versionZstdDictionaryMap.put(version, ByteBuffer.wrap(dictionary));
        compressionDictionaryFuture.complete(response);
      }
    });
    return compressionDictionaryFuture;
  }

  @Override
  public Schema getKeySchema() {
    return schemas.get().getKeySchema().getSchema();
  }

  @Override
  public Schema getValueSchema(int id) {
    return schemas.get().getValueSchema(id).getSchema();
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    SchemaEntry schemaEntry = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema);
    return schemas.get().getSchemaID(schemaEntry);
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemas.get().getValueSchema(getLatestValueSchemaId()).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    int latestValueSchemaId = latestSuperSetValueSchemaId.get();
    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = schemas.get().getMaxValueSchemaId();
    }
    return latestValueSchemaId;
  }
}
