package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static org.apache.http.HttpStatus.SC_OK;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.httpclient.VeniceMetaDataRequest;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DictionaryRetrievalService runs in a producer-consumer pattern. A thread is created which waits for items to be put
 * in a shared BlockingQueue.
 * There are 2 producers for the store versions to download dictionaries for.
 *  1) Store metadata changed ZK listener.
 *  2) Each failed dictionary fetch request is retried infinitely (till the version is retired).
 *
 * At Router startup, the dictionaries are pre-fetched for currently active versions that require a dictionary. This
 * process is fail-fast and will prevent router start up if any dictionary fetch request fails.
 *
 * When a dictionary is downloaded for a version, it's corresponding version specific compressor is initialized and is
 * maintained by CompressorFactory.
 */
public class DictionaryRetrievalService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(DictionaryRetrievalService.class);
  private static final int DEFAULT_DICTIONARY_DOWNLOAD_INTERVAL_IN_MS = 100;
  private final OnlineInstanceFinder onlineInstanceFinder;
  private final Optional<SSLFactory> sslFactory;
  private final ReadOnlyStoreRepository metadataRepository;
  private final Thread dictionaryRetrieverThread;
  private final ScheduledExecutorService executor;
  private final StorageNodeClient storageNodeClient;
  private final CompressorFactory compressorFactory;

  // Shared queue between producer and consumer where topics whose dictionaries have to be downloaded are put in.
  private final BlockingQueue<String> dictionaryDownloadCandidates = new LinkedBlockingQueue<>();

  // This map is used as a collection of futures that were created to download dictionaries for each store version.
  // The future's status also acts as an indicator of which dictionaries are currently active in memory.
  // 1) If an entry exists for the topic and it's state is "completed normally", it's dictionary has been downloaded.
  // 2) If an entry exists for the topic and it's state is "completed exceptionally", it's dictionary download failed.
  // The exception handler of that future is responsible for any retries.
  // 3) If an entry exists for the topic and it's state is "running", the dictionary download is currently in progress.
  // 4) If an entry doesn't exists for the topic, the version is unknown since it could have been retired/new or it
  // doesn't exist at all.
  private final VeniceConcurrentHashMap<String, CompletableFuture<Void>> downloadingDictionaryFutures =
      new VeniceConcurrentHashMap<>();

  private final int dictionaryRetrievalTimeMs;

  // This is the ZK Listener which acts as the primary producer for finding versions which require a dictionary.
  private final StoreDataChangedListener storeChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreCreated(Store store) {
      dictionaryDownloadCandidates.addAll(
          store.getVersions()
              .stream()
              .filter(
                  version -> version.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT
                      && version.getStatus() == VersionStatus.ONLINE)
              .map(Version::kafkaTopicName)
              .collect(Collectors.toList()));
    }

    @Override
    public void handleStoreDeleted(Store store) {
      store.getVersions().forEach(version -> handleVersionRetirement(version.kafkaTopicName(), "Store deleted."));
    }

    @Override
    public void handleStoreChanged(Store store) {
      List<Version> versions = store.getVersions();

      // For new versions, download dictionary.
      dictionaryDownloadCandidates.addAll(
          versions.stream()
              .filter(
                  version -> version.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT
                      && version.getStatus() == VersionStatus.ONLINE)
              .filter(version -> !downloadingDictionaryFutures.containsKey(version.kafkaTopicName()))
              .map(Version::kafkaTopicName)
              .collect(Collectors.toList()));

      // For versions that went into non ONLINE states, delete dictionary.
      versions.stream()
          .filter(
              version -> version.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT
                  && version.getStatus() != VersionStatus.ONLINE)
          .forEach(
              version -> handleVersionRetirement(version.kafkaTopicName(), "Version status " + version.getStatus()));

      // For versions that have been retired, delete dictionary.
      downloadingDictionaryFutures.keySet()
          .stream()
          .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(store.getName())) // Those topics which
                                                                                                // belong to the current
                                                                                                // store
          .filter(topic -> !store.getVersion(Version.parseVersionFromKafkaTopicName(topic)).isPresent()) // Those topics
                                                                                                         // which are
                                                                                                         // retired
          .forEach(topic -> handleVersionRetirement(topic, "Version retired"));
    }
  };

  /**
   *
   * @param onlineInstanceFinder OnlineInstanceFinder used to identify which storage node needs to be queried
   * @param routerConfig common router configuration
   * @param sslFactory if provided, the request will attempt to use ssl when fetching dictionary from the storage nodes
   */
  public DictionaryRetrievalService(
      OnlineInstanceFinder onlineInstanceFinder,
      VeniceRouterConfig routerConfig,
      Optional<SSLFactory> sslFactory,
      ReadOnlyStoreRepository metadataRepository,
      StorageNodeClient storageNodeClient,
      CompressorFactory compressorFactory) {
    this.onlineInstanceFinder = onlineInstanceFinder;
    this.sslFactory = sslFactory;
    this.metadataRepository = metadataRepository;
    this.storageNodeClient = storageNodeClient;
    this.compressorFactory = compressorFactory;

    // How long of a timeout we allow for a node to respond to a dictionary request
    dictionaryRetrievalTimeMs = routerConfig.getDictionaryRetrievalTimeMs();

    executor = Executors.newScheduledThreadPool(routerConfig.getRouterDictionaryProcessingThreads());

    // This thread is the consumer and it waits for an item to be put in the "dictionaryDownloadCandidates" queue.
    Runnable runnable = () -> {
      while (true) {
        String kafkaTopic;
        try {
          /**
           * In order to avoid retry storm; back off before querying server again.
           */
          kafkaTopic = dictionaryDownloadCandidates.take();
        } catch (InterruptedException e) {
          LOGGER.warn("Thread was interrupted while waiting for a candidate to download dictionary.", e);
          break;
        }

        // If the dictionary has already been downloaded, skip it.
        if (compressorFactory.versionSpecificCompressorExists(kafkaTopic)) {
          continue;
        }

        // If the dictionary is already being downloaded, skip it.
        if (downloadingDictionaryFutures.containsKey(kafkaTopic)) {
          continue;
        }

        downloadDictionaries(Arrays.asList(kafkaTopic));
      }
    };

    this.dictionaryRetrieverThread = new Thread(runnable);
  }

  private CompletableFuture<byte[]> getDictionary(String store, int version) {
    String kafkaTopic = Version.composeKafkaTopic(store, version);
    Instance instance = getOnlineInstance(kafkaTopic);

    if (instance == null) {
      return CompletableFuture.supplyAsync(() -> {
        throw new VeniceException("No online storage instance for resource: " + kafkaTopic);
      }, executor);
    }

    String instanceUrl = instance.getUrl(sslFactory.isPresent());

    LOGGER.info("Downloading dictionary for resource: {} from: {}", kafkaTopic, instanceUrl);

    VeniceMetaDataRequest request = new VeniceMetaDataRequest(
        instance,
        QueryAction.DICTIONARY.toString().toLowerCase() + "/" + store + "/" + version,
        HTTP_GET,
        sslFactory.isPresent());
    CompletableFuture<PortableHttpResponse> responseFuture = new CompletableFuture<>();

    storageNodeClient.sendRequest(request, responseFuture);

    return CompletableFuture.supplyAsync(() -> {
      VeniceException exception = null;
      try {
        byte[] dictionary = getDictionaryFromResponse(
            responseFuture.get(dictionaryRetrievalTimeMs, TimeUnit.MILLISECONDS),
            instanceUrl);
        if (dictionary == null) {
          exception = new VeniceException(
              "Dictionary download for resource: " + kafkaTopic + " from: " + instanceUrl
                  + " returned unexpected response.");
        } else {
          return dictionary;
        }
      } catch (InterruptedException e) {
        exception = new VeniceException(
            "Dictionary download for resource: " + kafkaTopic + " from: " + instanceUrl + " was interrupted: "
                + e.getMessage());
      } catch (ExecutionException e) {
        exception = new VeniceException(
            "ExecutionException encountered when downloading dictionary for resource: " + kafkaTopic + " from: "
                + instanceUrl + " : " + e.getMessage());
      } catch (TimeoutException e) {
        exception = new VeniceException(
            "Dictionary download for resource: " + kafkaTopic + " from: " + instanceUrl + " timed out : "
                + e.getMessage());
      }

      LOGGER.warn(exception.getMessage());

      throw exception;
    }, executor);
  }

  private byte[] getDictionaryFromResponse(PortableHttpResponse response, String instanceUrl) {
    try {
      int code = response.getStatusCode();
      if (code != SC_OK) {
        LOGGER.warn("Dictionary fetch returns {} for {}", code, instanceUrl);
      } else {
        ByteBuf byteBuf = response.getContentInByteBuf();
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        return bytes;
      }
    } catch (IOException e) {
      LOGGER.warn("Dictionary fetch HTTP response error: {} for {}", e.getMessage(), instanceUrl);
    }

    return null;
  }

  private Instance getOnlineInstance(String kafkaTopic) {
    try {
      int partitionCount = onlineInstanceFinder.getNumberOfPartitions(kafkaTopic);
      List<Instance> onlineInstances = new ArrayList<>();
      for (int p = 0; p < partitionCount; p++) {
        onlineInstances.addAll(onlineInstanceFinder.getReadyToServeInstances(kafkaTopic, p));
      }

      if (!onlineInstances.isEmpty()) {
        return onlineInstances.get((int) (Math.random() * onlineInstances.size()));
      }
    } catch (Exception e) {
      LOGGER.warn("Exception caught in getting online instances for resource: {}. {}", kafkaTopic, e.getMessage());
    }

    return null;
  }

  /**
   * At Router start up, we want dictionaries for all active versions to be downloaded. This call is a blocking call and
   * fails fast if there is a failure in fetching the dictionary for any version.
   * @return false if the dictionary download timed out, true otherwise.
   */
  private boolean getAllDictionaries() {
    metadataRepository.refresh();
    List<String> dictionaryDownloadCandidates = metadataRepository.getAllStores()
        .stream()
        .flatMap(store -> store.getVersions().stream())
        .filter(
            version -> version.getCompressionStrategy() == CompressionStrategy.ZSTD_WITH_DICT
                && version.getStatus() == VersionStatus.ONLINE)
        .filter(version -> !downloadingDictionaryFutures.containsKey(version.kafkaTopicName()))
        .map(Version::kafkaTopicName)
        .collect(Collectors.toList());

    return downloadDictionaries(dictionaryDownloadCandidates);
  }

  /**
   * This function downloads the dictionaries for the specified resources in a blocking manner.
   * @param dictionaryDownloadTopics A Collection of topics (representing store and version) to download the dictionaries for.
   * @return false if the dictionary download timed out, true otherwise.
   */
  private boolean downloadDictionaries(Collection<String> dictionaryDownloadTopics) {
    String storeTopics = String.join(",", dictionaryDownloadTopics);
    if (storeTopics.isEmpty()) {
      return true;
    }

    List<Version> filteredTopics = dictionaryDownloadTopics.stream()
        .map(
            topic -> metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(topic))
                .getVersion(Version.parseVersionFromKafkaTopicName(topic)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    LOGGER.info("Beginning dictionary fetch for {}", storeTopics);

    CompletableFuture[] dictionaryDownloadFutureArray = filteredTopics.stream()
        .map(this::fetchCompressionDictionary)
        .filter(Objects::nonNull)
        .toArray(CompletableFuture[]::new);

    try {
      CompletableFuture.allOf(dictionaryDownloadFutureArray).get(dictionaryRetrievalTimeMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOGGER.warn("Dictionary fetch failed. Store topics were: {}. {}", storeTopics, e.getMessage());
      return false;
    }
    return true;
  }

  private CompletableFuture<Void> fetchCompressionDictionary(Version version) {
    String kafkaTopic = version.kafkaTopicName();

    CompletableFuture<Void> dictionaryFuture;
    if (downloadingDictionaryFutures.containsKey(kafkaTopic)) {
      dictionaryFuture = downloadingDictionaryFutures.get(kafkaTopic);
    } else {
      dictionaryFuture =
          getDictionary(version.getStoreName(), version.getNumber()).handleAsync((dictionary, exception) -> {
            if (exception != null) {
              if (exception instanceof InterruptedException) {
                LOGGER.warn("{}. Will not retry dictionary download.", exception.getMessage());
              } else {
                LOGGER.warn(
                    "Exception encountered when asynchronously downloading dictionary for resource: {}. {}",
                    kafkaTopic,
                    exception.getMessage());

                // Wait for future to be added before removing it
                while (downloadingDictionaryFutures.remove(kafkaTopic) == null) {
                  if (!Utils.sleep(DEFAULT_DICTIONARY_DOWNLOAD_INTERVAL_IN_MS)) {
                    LOGGER.warn("Got InterruptedException. Will not retry dictionary download.");
                    return null;
                  }
                }

                executor.schedule(
                    () -> dictionaryDownloadCandidates.add(kafkaTopic),
                    DEFAULT_DICTIONARY_DOWNLOAD_INTERVAL_IN_MS,
                    TimeUnit.MILLISECONDS);
              }
            } else {
              initCompressorFromDictionary(version, dictionary);
              LOGGER.info("Dictionary downloaded and compressor is ready for resource: {}", kafkaTopic);
            }
            return null;
          }, executor);
      downloadingDictionaryFutures.put(kafkaTopic, dictionaryFuture);
    }

    return dictionaryFuture;
  }

  private void initCompressorFromDictionary(Version version, byte[] dictionary) {
    String kafkaTopic = version.kafkaTopicName();
    if (version.getStatus() != VersionStatus.ONLINE || !downloadingDictionaryFutures.containsKey(kafkaTopic)) {
      // Nothing to do since version was retired.
      return;
    }
    CompressionStrategy compressionStrategy = version.getCompressionStrategy();
    compressorFactory.createVersionSpecificCompressorIfNotExist(compressionStrategy, kafkaTopic, dictionary);
  }

  private void handleVersionRetirement(String kafkaTopic, String exceptionReason) {
    InterruptedException e =
        new InterruptedException("Dictionary download for resource " + kafkaTopic + " interrupted: " + exceptionReason);
    CompletableFuture<Void> dictionaryFutureForTopic = downloadingDictionaryFutures.remove(kafkaTopic);
    if (dictionaryFutureForTopic != null && !dictionaryFutureForTopic.isDone()) {
      dictionaryFutureForTopic.completeExceptionally(e);
    }
    dictionaryDownloadCandidates.remove(kafkaTopic);
    compressorFactory.removeVersionSpecificCompressor(kafkaTopic);
  }

  @Override
  public boolean startInner() {
    metadataRepository.registerStoreDataChangedListener(storeChangeListener);
    // Dictionary warmup
    boolean success = getAllDictionaries();
    // If dictionary warm up failed, stop router from starting up
    if (!success) {
      throw new VeniceException("Dictionary warmup failed! Preventing router start up.");
    }
    dictionaryRetrieverThread.start();
    return true;
  }

  @Override
  public void stopInner() throws IOException {
    dictionaryRetrieverThread.interrupt();
    executor.shutdownNow();
    downloadingDictionaryFutures.forEach(
        (topic, future) -> future
            .completeExceptionally(new InterruptedException("Dictionary download thread stopped")));
  }
}
