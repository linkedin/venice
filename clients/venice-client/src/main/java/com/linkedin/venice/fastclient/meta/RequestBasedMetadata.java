package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig;
import static com.linkedin.venice.schema.SchemaData.INVALID_VALUE_SCHEMA_ID;

import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Store metadata class that uses the server's endpoint to fetch metadata and keep the local cache up to date.
 */
public class RequestBasedMetadata extends AbstractStoreMetadata {
  private static final Logger LOGGER = LogManager.getLogger(RequestBasedMetadata.class);
  private static final String VERSION_PARTITION_SEPARATOR = "_";

  public static final long DEFAULT_REFRESH_INTERVAL_IN_SECONDS = 60;
  private static final long ZSTD_DICT_FETCH_TIMEOUT_IN_SECONDS = 10;
  public static final long DEFAULT_CONN_WARMUP_TIMEOUT_IN_SECONDS_DEFAULT = 20;
  static final long INITIAL_METADATA_FETCH_REFRESH_INTERVAL_IN_SECONDS = 5;

  private long refreshIntervalInSeconds;
  private final boolean isMetadataConnWarmupEnabled;
  private final long connWarmupTimeoutInSeconds;
  /** scheduler to run {@link #refresh()} to periodically update metadata */
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  /** scheduler within {@link #refresh()} to warmup new instances updated via metadata refresh.
   * Using a new ExecutorService rather than CompletableFuture's default one to not affect
   * the read requests happening in parallel.
   * Using newCachedThreadPool to avoid idle threads most of the time and to be able to warm up connections to
   * multiple instances quickly when needed (e.g., at the beginning of a client start or when
   * there is a new version or migration)
   */
  private ExecutorService h2ConnWarmupExecutorService =
      Executors.newCachedThreadPool(new NamedThreadFactory("h2ConnWarmupForFastClient"));

  private final AtomicInteger currentVersion = new AtomicInteger();
  private final AtomicInteger latestSuperSetValueSchemaId = new AtomicInteger();
  private final AtomicReference<SchemaData> schemas = new AtomicReference<>();
  private final Map<String, List<String>> readyToServeInstancesMap = new VeniceConcurrentHashMap<>();
  private Map<String, CompletableFuture> warmUpInstancesFutures = new VeniceConcurrentHashMap<>();
  private final Map<Integer, VenicePartitioner> versionPartitionerMap = new VeniceConcurrentHashMap<>();
  private final Map<Integer, Integer> versionPartitionCountMap = new VeniceConcurrentHashMap<>();
  private final Map<Integer, ByteBuffer> versionZstdDictionaryMap = new VeniceConcurrentHashMap<>();
  private final Map<String, Integer> helixGroupInfo = new VeniceConcurrentHashMap<>();
  private final CompressorFactory compressorFactory;
  private final D2TransportClient d2TransportClient;
  private final R2TransportClient r2TransportClient;
  private D2ServiceDiscovery d2ServiceDiscovery;
  private final String clusterDiscoveryD2ServiceName;
  private final ClusterStats clusterStats;
  private final FastClientStats clientStats;
  private final AtomicInteger batchGetLimit = new AtomicInteger();
  private RouterBackedSchemaReader metadataResponseSchemaReader;
  private volatile boolean isServiceDiscovered;
  private volatile boolean isReady;
  private CountDownLatch isReadyLatch = new CountDownLatch(1);
  private AtomicReference<String> serverClusterName = new AtomicReference<>();

  private Set<String> harClusters;

  public RequestBasedMetadata(ClientConfig clientConfig, D2TransportClient d2TransportClient) {
    super(clientConfig);
    this.isMetadataConnWarmupEnabled = clientConfig.isMetadataConnWarmupEnabled();
    this.refreshIntervalInSeconds = clientConfig.getMetadataRefreshIntervalInSeconds() > 0
        ? clientConfig.getMetadataRefreshIntervalInSeconds()
        : DEFAULT_REFRESH_INTERVAL_IN_SECONDS;
    this.connWarmupTimeoutInSeconds = clientConfig.getMetadataConnWarmupTimeoutInSeconds() > 0
        ? clientConfig.getMetadataConnWarmupTimeoutInSeconds()
        : DEFAULT_CONN_WARMUP_TIMEOUT_IN_SECONDS_DEFAULT;
    this.d2TransportClient = d2TransportClient;
    this.d2ServiceDiscovery = new D2ServiceDiscovery();
    this.clusterDiscoveryD2ServiceName = d2TransportClient.getServiceName();
    this.compressorFactory = new CompressorFactory();
    this.clusterStats = clientConfig.getClusterStats();
    this.clientStats = clientConfig.getStats(RequestType.SINGLE_GET);
    InternalAvroStoreClient metadataSchemaResponseStoreClient = new AvroGenericStoreClientImpl(
        // Create a new D2TransportClient since the other one will be set to point to server d2 after cluster discovery
        new D2TransportClient(clusterDiscoveryD2ServiceName, d2TransportClient.getD2Client()),
        false,
        defaultGenericClientConfig(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName()));
    this.metadataResponseSchemaReader =
        new RouterBackedSchemaReader(() -> metadataSchemaResponseStoreClient, Optional.empty(), Optional.empty());
    this.r2TransportClient = new R2TransportClient(clientConfig.getR2Client());
    this.harClusters = clientConfig.getHarClusters();
  }

  // For unit tests only
  synchronized void setMetadataResponseSchemaReader(RouterBackedSchemaReader metadataResponseSchemaReader) {
    this.metadataResponseSchemaReader = metadataResponseSchemaReader;
  }

  @Override
  public String getClusterName() {
    return serverClusterName.get();
  }

  @Override
  public int getCurrentStoreVersion() {
    if (!isReady()) {
      throw new VeniceClientException(getStoreName() + " metadata is not ready yet, retry in sometime");
    }
    return currentVersion.get();
  }

  @Override
  public int getPartitionId(int version, ByteBuffer key) {
    VenicePartitioner partitioner = versionPartitionerMap.get(version);
    if (partitioner == null) {
      throw new VeniceClientException("Unknown version number: " + version + " for store: " + storeName);
    }
    return partitioner.getPartitionId(key, versionPartitionCountMap.get(version));
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    String key = getVersionPartitionMapKey(version, partitionId);
    return readyToServeInstancesMap.getOrDefault(key, Collections.emptyList());
  }

  private String getVersionPartitionMapKey(int version, int partition) {
    return version + VERSION_PARTITION_SEPARATOR + partition;
  }

  /**
   * Given a version partitioner key formatted as "1_0" which represents partition 0 of version 1, return the version
   * number.
   * @param key
   * @return version number
   */
  private int getVersionFromKey(String key) {
    return Integer.parseInt(key.split(VERSION_PARTITION_SEPARATOR)[0]);
  }

  @Override
  public void start() {
    // perform cluster discovery work upfront and retrieve the server d2 service name
    discoverD2Service();

    // build a base for future metadata updates then start periodic refresh
    try {
      refresh();
    } catch (VeniceClientHttpException clientHttpException) {
      if (clientHttpException.getHttpStatus() == HttpStatus.SC_FORBIDDEN) {
        LOGGER.error(clientHttpException.getMessage());
        throw new ConfigurationException(clientHttpException.getMessage());
      }
    }
    try {
      // wait till metadata is fetched for the first time
      isReadyLatch.await();
    } catch (InterruptedException e) {
      // if there is an InterruptedException, let the periodic refresh continue with updating the metadata
      LOGGER.error(
          "Metadata refresh failed and will be retried every {} seconds. Read requests will throw exception until then",
          INITIAL_METADATA_FETCH_REFRESH_INTERVAL_IN_SECONDS,
          e);
    }
  }

  void discoverD2Service() {
    if (isServiceDiscovered) {
      return;
    }
    synchronized (this) {
      if (isServiceDiscovered) {
        return;
      }
      d2TransportClient.setServiceName(clusterDiscoveryD2ServiceName);
      String serverD2ServiceName = d2ServiceDiscovery.find(d2TransportClient, storeName, true).getServerD2Service();
      d2TransportClient.setServiceName(serverD2ServiceName);
      serverClusterName.set(serverD2ServiceName);
      isServiceDiscovered = true;
      if (harClusters.contains(serverD2ServiceName)) {
        LOGGER.info(
            "Server cluster: {} has HAR enabled, so client will switch to the following routing strategy: {}",
            serverD2ServiceName,
            ClientRoutingStrategyType.HELIX_ASSISTED);
        setRoutingStrategy(ClientRoutingStrategyType.HELIX_ASSISTED);
      }
    }
  }

  /**
   * Warmup the connection from the client to Instances discovered from the metadata update.
   * To avoid duplicate warmup requests being sent which might delay the warmup process,
   * {@link #warmUpInstancesFutures} is used to track the list of already warmed up instances
   * and its status.
   * <p>
   * As we are not trying to validate the metadata here but just to warmup conns beforehand,
   * warmup is on best effort basis for now. If we want to improve the long tail and potentially
   * further improve the client startup speed by not waiting till the timeout for all the conns
   * to be warmed up, we can think of adding more fine-grained logic wrt partitions, replicas or
   * warmup success rate, which might succeed faster that waiting for all conns.
   */
  private void warmupConnectionToInstances(int currentFetchedVersion, int partitionCount) throws ExecutionException {
    Set<String> newReplicasToBeWarmedUp = new HashSet<>();
    // get all replicas
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      newReplicasToBeWarmedUp.addAll(getReplicas(currentFetchedVersion, partitionId));
    }
    String logPrefix =
        String.format("Metadata connection warmup for store %s version %d:", storeName, currentFetchedVersion);
    LOGGER.info("{} Newly fetched metadata has {} instances", logPrefix, newReplicasToBeWarmedUp.size());

    // If there are warmup in progress, check its status to cancel or reuse the same warmup
    Set<String> replicasWarmupInProgressFromPastRefresh = new HashSet<>();
    int warmupSuccessfullInPastRefresh = 0;
    Iterator<Map.Entry<String, CompletableFuture>> entryIterator = warmUpInstancesFutures.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, CompletableFuture> entry = entryIterator.next();
      String replica = entry.getKey();
      CompletableFuture<Void> future = entry.getValue();
      if (!newReplicasToBeWarmedUp.contains(replica)) {
        // No need to warm up this instance anymore as it's not in the latest metadata: cancel if already running
        if (!future.isDone()) {
          future.cancel(true);
        }
        entryIterator.remove();
      } else {
        // Already attempted to warmup in the past warmups.
        if (!future.isDone()) {
          // not restarting it: in the worst case, the first read request to this instance will try it again.
          replicasWarmupInProgressFromPastRefresh.add(replica);
          newReplicasToBeWarmedUp.remove(replica);
        } else if (!future.isCompletedExceptionally()) {
          // no need to warm up these instances again as its already completed successfully in the past warmups.
          newReplicasToBeWarmedUp.remove(replica);
          warmupSuccessfullInPastRefresh++;
        } else {
          // else will be warmed up again
          entryIterator.remove();
        }
      }
    }

    if (replicasWarmupInProgressFromPastRefresh.size() != 0) {
      LOGGER.warn(
          "{} {} instances still incomplete from the previous attempt, not retrying again for these instances: {}",
          logPrefix,
          replicasWarmupInProgressFromPastRefresh.size(),
          String.join(",", replicasWarmupInProgressFromPastRefresh));
    }

    // Warmup all the new instances
    int instanceNum = newReplicasToBeWarmedUp.size();
    if (instanceNum == 0) {
      if (replicasWarmupInProgressFromPastRefresh.size() == 0) {
        LOGGER.info("{} all instances are already warmed up in the previous attempts", logPrefix);
      }
      return;
    }

    if (warmupSuccessfullInPastRefresh != 0) {
      LOGGER.info(
          "{} {} instances already warmed up in the previous attempts, Starting H2 connection warm up from fast client to the remaining {} instances",
          logPrefix,
          warmupSuccessfullInPastRefresh,
          instanceNum);
    } else {
      LOGGER.info("{} Starting H2 connection warm up from fast client to {} instances", logPrefix, instanceNum);
    }

    AtomicInteger numberOfWarmUpSuccess = new AtomicInteger(0);
    Set<String> warmUpFailedInstances = VeniceConcurrentHashMap.newKeySet();
    newReplicasToBeWarmedUp.forEach((replica) -> {
      String url = replica + "/" + QueryAction.HEALTH.toString().toLowerCase();
      CompletableFuture warmupFuture = CompletableFuture.runAsync(() -> {
        try {
          r2TransportClient.get(url).get();
          numberOfWarmUpSuccess.incrementAndGet();
        } catch (Exception e) {
          warmUpFailedInstances.add(replica);
          throw new RuntimeException(String.format("%s warmup failed for replica: %s", logPrefix, replica), e);
        }
      }, h2ConnWarmupExecutorService);

      warmupFuture.whenComplete((ignore, throwable) -> {
        if (throwable != null) {
          warmupFuture.completeExceptionally((Throwable) throwable);
        }
      });
      warmUpInstancesFutures.put(replica, warmupFuture);
    });

    CompletableFuture<Void> warmupResultFuture =
        CompletableFuture.allOf(warmUpInstancesFutures.values().toArray(new CompletableFuture[0]));
    try {
      warmupResultFuture.get(connWarmupTimeoutInSeconds, TimeUnit.SECONDS);
      LOGGER.info("{} {} instances succeeded", logPrefix, numberOfWarmUpSuccess.get());
    } catch (Exception e) {
      int successCount = numberOfWarmUpSuccess.get();
      int failureCount = warmUpFailedInstances.size();
      if (e instanceof TimeoutException) {
        int notCompletedCount = instanceNum - (successCount + failureCount);
        LOGGER.warn(
            "{} {} instances succeeded, {} instances failed and {} instances failed to finish "
                + "within {} seconds. Failed instances {} will be retried during the periodic metadata refresh.",
            logPrefix,
            successCount,
            failureCount,
            notCompletedCount,
            connWarmupTimeoutInSeconds,
            String.join(",", warmUpFailedInstances));
      } else {
        LOGGER.warn(
            "{} {} instances succeeded, {} instances failed. Failed instances {} will be retried "
                + "during the periodic metadata refresh.",
            logPrefix,
            successCount,
            failureCount,
            String.join(",", warmUpFailedInstances),
            e);
      }
    }
  }

  /**
   * Update is only performed if the version from the fetched metadata is different from the local version. We evict
   * old values as we perform updates, while making sure we keep all currently active versions.
   * @param onDemandRefresh
   * @return if the fetched metadata was an updated version
   */
  synchronized void updateCache(boolean onDemandRefresh) throws InterruptedException {
    LOGGER.debug("Metadata fetch operation for store: {} started", storeName);
    long currentTimeMs = System.currentTimeMillis();
    // call the METADATA endpoint
    try {
      TransportClientResponse transportClientResponse = fetchMetadata().get();
      // Metadata response schema forward compatibility support via router backed schema reader
      int writerSchemaId = transportClientResponse.getSchemaId();
      Schema writerSchema = metadataResponseSchemaReader.getValueSchema(writerSchemaId);
      byte[] body = transportClientResponse.getBody();
      RecordDeserializer<MetadataResponseRecord> metadataResponseDeserializer =
          FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(writerSchema, MetadataResponseRecord.class);
      MetadataResponseRecord metadataResponse = metadataResponseDeserializer.deserialize(body);
      VersionProperties versionMetadata = metadataResponse.getVersionMetadata();
      batchGetLimit.set(metadataResponse.getBatchGetLimit());
      int fetchedCurrentVersion = versionMetadata.getCurrentVersion();

      // call the DICTIONARY endpoint if needed
      CompletableFuture<TransportClientResponse> dictionaryFetchFuture = null;
      if (!versionZstdDictionaryMap.containsKey(fetchedCurrentVersion)
          && versionMetadata.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT.getValue()) {
        dictionaryFetchFuture = fetchCompressionDictionary(fetchedCurrentVersion);
      }

      // Update partitioner pair map (versionPartitionerMap)
      int partitionCount = versionMetadata.getPartitionCount();
      Properties params = new Properties();
      params.putAll(versionMetadata.getPartitionerParams());
      VenicePartitioner partitioner = PartitionUtils
          .getVenicePartitioner(versionMetadata.getPartitionerClass().toString(), new VeniceProperties(params));
      versionPartitionerMap.put(fetchedCurrentVersion, partitioner);
      versionPartitionCountMap.put(fetchedCurrentVersion, partitionCount);

      // Update readyToServeInstanceMap
      Map<Integer, List<String>> routingInfo = metadataResponse.getRoutingInfo()
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  e -> Integer.valueOf(e.getKey().toString()),
                  e -> e.getValue().stream().map(CharSequence::toString).collect(Collectors.toList())));

      for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
        String key = getVersionPartitionMapKey(fetchedCurrentVersion, partitionId);
        readyToServeInstancesMap.put(key, routingInfo.get(partitionId));
      }

      // Update schemas
      Map.Entry<CharSequence, CharSequence> lastEntry = null;
      for (Map.Entry<CharSequence, CharSequence> entry: metadataResponse.getKeySchema().entrySet()) {
        lastEntry = entry;
      }
      SchemaEntry keySchema = lastEntry == null
          ? null
          : new SchemaEntry(Integer.parseInt(lastEntry.getKey().toString()), lastEntry.getValue().toString());
      SchemaData schemaData = new SchemaData(storeName, keySchema);
      for (Map.Entry<CharSequence, CharSequence> entry: metadataResponse.getValueSchemas().entrySet()) {
        schemaData
            .addValueSchema(new SchemaEntry(Integer.parseInt(entry.getKey().toString()), entry.getValue().toString()));
      }
      schemas.set(schemaData);

      // Update helix group info
      for (Map.Entry<CharSequence, Integer> entry: metadataResponse.getHelixGroupInfo().entrySet()) {
        helixGroupInfo.put(entry.getKey().toString(), entry.getValue());
      }

      latestSuperSetValueSchemaId.set(metadataResponse.getLatestSuperSetValueSchemaId());
      // Wait for dictionary fetch to finish if there is one
      try {
        if (dictionaryFetchFuture != null) {
          dictionaryFetchFuture.get(ZSTD_DICT_FETCH_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        }
      } catch (ExecutionException | TimeoutException e) {
        LOGGER.warn(
            "Dictionary fetch operation could not complete in time for some of the versions. "
                + "Will be retried on next refresh",
            e);
        // throw exception to make the start() blocking till the dictionary is fetched in case
        // of initial fetch. For other cases: returning an exception doesn't make any difference.
        throw new VeniceException(e);
      }

      if (isMetadataConnWarmupEnabled) {
        // warmup H2 conns before setting the fetched version as the current version
        warmupConnectionToInstances(fetchedCurrentVersion, partitionCount);
      }

      // Evict entries from inactive versions
      Set<Integer> activeVersions = new HashSet<>(metadataResponse.getVersions());
      readyToServeInstancesMap.entrySet()
          .removeIf(entry -> !activeVersions.contains(getVersionFromKey(entry.getKey())));
      versionPartitionerMap.entrySet().removeIf(entry -> !activeVersions.contains(entry.getKey()));
      versionPartitionCountMap.entrySet().removeIf(entry -> !activeVersions.contains(entry.getKey()));
      versionZstdDictionaryMap.entrySet().removeIf(entry -> !activeVersions.contains(entry.getKey()));
      currentVersion.set(fetchedCurrentVersion);
      clusterStats.updateCurrentVersion(fetchedCurrentVersion);
      routingStrategy.updateHelixGroupInfo(helixGroupInfo);
      // Update the metadata timestamp only if all updates are successful
      clientStats.updateCacheTimestamp(currentTimeMs);
      LOGGER.debug(
          "Metadata fetch operation for store: {} finished successfully with current version {}.",
          storeName,
          fetchedCurrentVersion);
    } catch (ExecutionException e) {
      // perform an on demand refresh if update fails in case of store migration
      // TODO: need a better way to handle store migration
      if (!onDemandRefresh) {
        LOGGER.warn("Metadata fetch operation for store: {} failed with exception {}", storeName, e.getMessage());
        isServiceDiscovered = false;
        discoverD2Service();
        updateCache(true);
      } else {
        // pass the error along if the on demand refresh also fails
        clusterStats.recordVersionUpdateFailure();
        throw new VeniceClientException(
            String.format("Metadata fetch operation for store: %s retry failed", storeName),
            e.getCause());
      }
    }
  }

  private void refresh() {
    try {
      updateCache(false);
      if (!isReady) {
        isReadyLatch.countDown();
        isReady = true;
        LOGGER.info("Metadata initial fetch completed successfully for store: {}", storeName);
      }
    } catch (VeniceClientException clientException) {
      if (clientException.getCause() instanceof VeniceClientHttpException) {
        VeniceClientHttpException clientHttpException = (VeniceClientHttpException) clientException.getCause();
        if (clientHttpException.getHttpStatus() == HttpStatus.SC_FORBIDDEN) {
          throw clientHttpException;
        }
      }
      logRefreshException(clientException);
    } catch (Exception e) {
      // Catch all errors so periodic refresh doesn't break on transient errors.
      logRefreshException(e);
    } finally {
      scheduler.schedule(
          this::refresh,
          isReady ? refreshIntervalInSeconds : INITIAL_METADATA_FETCH_REFRESH_INTERVAL_IN_SECONDS,
          TimeUnit.SECONDS);
    }
  }

  private void logRefreshException(Exception e) {
    LOGGER.error(
        "Metadata periodic refresh for store: {} encountered unexpected error, will be retried in {} seconds",
        storeName,
        isReady ? refreshIntervalInSeconds : INITIAL_METADATA_FETCH_REFRESH_INTERVAL_IN_SECONDS,
        e);
  }

  @Override
  public void close() throws IOException {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    h2ConnWarmupExecutorService.shutdown();
    try {
      if (!h2ConnWarmupExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
        h2ConnWarmupExecutorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    readyToServeInstancesMap.clear();
    versionPartitionerMap.clear();
    Utils.closeQuietlyWithErrorLogged(metadataResponseSchemaReader);
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  private CompletableFuture<TransportClientResponse> fetchMetadata() {
    CompletableFuture<TransportClientResponse> metadataFuture = new CompletableFuture<>();
    String url = QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;

    LOGGER.debug("Fetching metadata for store {} from URL {} ", storeName, url);
    d2TransportClient.get(url).whenComplete((response, throwable) -> {
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
    String url = QueryAction.DICTIONARY.toString().toLowerCase() + "/" + storeName + "/" + version;

    LOGGER.debug("Fetching compression dictionary for version {} from URL {} ", version, url);
    d2TransportClient.get(url).whenComplete((response, throwable) -> {
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
  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
    return getCompressor(compressionStrategy, version, compressorFactory, versionZstdDictionaryMap);
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
    SchemaEntry schemaEntry = new SchemaEntry(INVALID_VALUE_SCHEMA_ID, schema);
    return schemas.get().getSchemaID(schemaEntry);
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemas.get().getValueSchema(getLatestValueSchemaId()).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    int latestValueSchemaId = latestSuperSetValueSchemaId.get();
    if (latestValueSchemaId == INVALID_VALUE_SCHEMA_ID) {
      latestValueSchemaId = schemas.get().getMaxValueSchemaId();
    }
    return latestValueSchemaId;
  }

  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    // Ideally, we can fetch this information from the SchemaData object, but we're not yet populating these schemas
    throw new VeniceUnsupportedOperationException("getUpdateSchema");
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    // Ideally, we can fetch this information from the SchemaData object, but we're not yet populating these schemas
    throw new VeniceUnsupportedOperationException("getLatestUpdateSchema");
  }

  @Override
  public boolean isReady() {
    return isReady;
  }

  @Override
  public int getBatchGetLimit() {
    return batchGetLimit.get();
  }

  /**
   * Used for test only
   * @param d2ServiceDiscovery
   */
  public synchronized void setD2ServiceDiscovery(D2ServiceDiscovery d2ServiceDiscovery) {
    this.d2ServiceDiscovery = d2ServiceDiscovery;
  }

  public void setScheduler(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  public void setIsReadyLatch(CountDownLatch isReadyLatch) {
    this.isReadyLatch = isReadyLatch;
  }

  public CountDownLatch getIsReadyLatch() {
    return this.isReadyLatch;
  }

  public void setRefreshIntervalInSeconds(long refreshIntervalInSeconds) {
    this.refreshIntervalInSeconds = refreshIntervalInSeconds;
  }

  public long getRefreshIntervalInSeconds() {
    return refreshIntervalInSeconds;
  }

  public Map<String, CompletableFuture> getWarmUpInstancesFutures() {
    return warmUpInstancesFutures;
  }

  public void setWarmUpInstancesFutures(Map<String, CompletableFuture> warmUpInstancesFutures) {
    this.warmUpInstancesFutures = warmUpInstancesFutures;
  }
}
