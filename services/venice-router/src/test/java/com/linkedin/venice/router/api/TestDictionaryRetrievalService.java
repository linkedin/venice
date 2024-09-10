package com.linkedin.venice.router.api;

import static com.linkedin.venice.router.api.DictionaryRetrievalService.MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS;
import static com.linkedin.venice.router.api.DictionaryRetrievalService.MIN_DICTIONARY_DOWNLOAD_DELAY_TIME_MS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDictionaryRetrievalService {
  OnlineInstanceFinder onlineInstanceFinder;
  VeniceRouterConfig routerConfig;
  ReadOnlyStoreRepository metadataRepository;
  SSLFactory sslFactory;
  StorageNodeClient storageNodeClient;
  CompressorFactory compressorFactory;
  Store store;
  Version version;

  private static String STORE_NAME = "test_store";
  private static int VERSION_NUMBER = 1;
  private static String KAFKA_TOPIC_NAME = STORE_NAME + "_v1";

  @BeforeMethod
  public void setUp() {
    onlineInstanceFinder = mock(OnlineInstanceFinder.class);
    routerConfig = mock(VeniceRouterConfig.class);
    metadataRepository = mock(ReadOnlyStoreRepository.class);
    List<Store> stores = new ArrayList();
    store = mock(Store.class);
    doReturn(STORE_NAME).when(store).getName();
    List<Version> versions = new ArrayList();
    version = mock(Version.class);
    // start with NO_OP for warmup to succeed
    doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn(KAFKA_TOPIC_NAME).when(version).kafkaTopicName();
    versions.add(version);
    doReturn(versions).when(store).getVersions();
    doReturn(version).when(store).getVersion(VERSION_NUMBER);
    stores.add(store);
    doReturn(stores).when(metadataRepository).getAllStores();
    doReturn(store).when(metadataRepository).getStore(any());
    storageNodeClient = mock(StorageNodeClient.class);
    compressorFactory = mock(CompressorFactory.class);
    sslFactory = mock(SSLFactory.class);
  }

  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testSuppressingRedundantLogs() throws Exception {
    DictionaryRetrievalService dictionaryRetrievalService = null;
    try {
      dictionaryRetrievalService = new DictionaryRetrievalService(
          onlineInstanceFinder,
          routerConfig,
          Optional.of(sslFactory),
          metadataRepository,
          storageNodeClient,
          compressorFactory);
      dictionaryRetrievalService.start();
      StoreDataChangedListener storeChangeListener = dictionaryRetrievalService.getStoreChangeListener();
      // update the compression strategy such that dictionaryRetrievalService will try to fetch the dictionary
      doReturn(CompressionStrategy.ZSTD_WITH_DICT).when(version).getCompressionStrategy();
      storeChangeListener.handleStoreChanged(store);
      RedundantExceptionFilter redundantExceptionFilter = dictionaryRetrievalService.getRedundantExceptionFilter();

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        assertTrue(
            redundantExceptionFilter.isRedundantException("Beginning dictionary fetch for " + KAFKA_TOPIC_NAME, false));
        assertTrue(
            redundantExceptionFilter.isRedundantException(
                "Dictionary fetch failed. Store topics were: " + KAFKA_TOPIC_NAME + ". null",
                false));
        assertTrue(
            redundantExceptionFilter.isRedundantException(
                "Exception encountered when asynchronously downloading dictionary for resource: " + KAFKA_TOPIC_NAME
                    + ". com.linkedin.venice.exceptions.VeniceException: No online storage instance for resource: null_v0",
                false));
      });
    } finally {
      if (dictionaryRetrievalService != null) {
        dictionaryRetrievalService.stop();
        dictionaryRetrievalService.close();
        // reset to NO_OP for next test
        doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
      }
    }
  }

  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testExponentialRetryDelay() throws Exception {
    DictionaryRetrievalService dictionaryRetrievalService = null;

    try {
      dictionaryRetrievalService = new DictionaryRetrievalService(
          onlineInstanceFinder,
          routerConfig,
          Optional.of(sslFactory),
          metadataRepository,
          storageNodeClient,
          compressorFactory);
      dictionaryRetrievalService.start();
      StoreDataChangedListener storeChangeListener = dictionaryRetrievalService.getStoreChangeListener();

      // Update the compression strategy such that dictionaryRetrievalService will try to fetch the dictionary
      doReturn(CompressionStrategy.ZSTD_WITH_DICT).when(version).getCompressionStrategy();
      storeChangeListener.handleStoreChanged(store);

      DictionaryRetrievalService finalDictionaryRetrievalService = dictionaryRetrievalService;
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertNotNull(finalDictionaryRetrievalService.getFetchDelayTimeinMsMap().get(KAFKA_TOPIC_NAME));
      });

      // start at a higher retry time as it will be easy to miss the first couple of retries
      long expectedValue = MIN_DICTIONARY_DOWNLOAD_DELAY_TIME_MS * 4;
      while (true) {
        long finalExpectedValue = expectedValue;
        TestUtils.waitForNonDeterministicAssertion(MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS, TimeUnit.SECONDS, () -> {
          assertEquals(
              (long) finalDictionaryRetrievalService.getFetchDelayTimeinMsMap().get(KAFKA_TOPIC_NAME),
              finalExpectedValue);
        });
        if (expectedValue == MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS) {
          break;
        }
        expectedValue *= 2;
        if (expectedValue > MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS) {
          expectedValue = MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS;
        }
      }
    } finally {
      if (dictionaryRetrievalService != null) {
        dictionaryRetrievalService.stop();
        dictionaryRetrievalService.close();
        // Reset to NO_OP for the next test
        doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
      }
    }
  }

  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testStoreChangeCallbackDoNotRemoveExistingVersions() throws Exception {
    DictionaryRetrievalService dictionaryRetrievalService = null;
    try {
      dictionaryRetrievalService = new DictionaryRetrievalService(
          onlineInstanceFinder,
          routerConfig,
          Optional.of(sslFactory),
          metadataRepository,
          storageNodeClient,
          compressorFactory);
      dictionaryRetrievalService.start();
      StoreDataChangedListener storeChangeListener = dictionaryRetrievalService.getStoreChangeListener();
      // update the compression strategy such that dictionaryRetrievalService will try to fetch the dictionary
      doReturn(CompressionStrategy.ZSTD_WITH_DICT).when(version).getCompressionStrategy();
      doReturn(VersionStatus.ONLINE).when(version).getStatus();
      doReturn(STORE_NAME).when(version).getStoreName();
      doReturn(VERSION_NUMBER).when(version).getNumber();
      // Ensure that compressor does not exist for this version yet
      doReturn(false).when(compressorFactory).versionSpecificCompressorExists(KAFKA_TOPIC_NAME);

      doReturn(1).when(onlineInstanceFinder).getNumberOfPartitions(KAFKA_TOPIC_NAME);
      Instance mockInstance = mock(Instance.class);
      doReturn("localhost").when(mockInstance).getUrl(anyBoolean());
      List<Instance> instances = new ArrayList<>();
      instances.add(mockInstance);
      doReturn(instances).when(onlineInstanceFinder).getReadyToServeInstances(KAFKA_TOPIC_NAME, 0);
      // Create the store first to trigger dictionary download future
      storeChangeListener.handleStoreCreated(store);
      // Block in the storage client so that future will not fail too fast to allow the validation for future maps
      doAnswer(invocation -> {
        Thread.sleep(10000); // Sleep for 10 seconds
        return null; // Return null because the method is void
      }).when(storageNodeClient).sendRequest(any(), any());
      // Do a no-op store change to execute the dictionary future clean-up logic
      storeChangeListener.handleStoreChanged(store);
      Map<String, CompletableFuture<Void>> downloadingDictionaryFutures =
          dictionaryRetrievalService.getDownloadingDictionaryFutures();
      TestUtils.waitForNonDeterministicAssertion(MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS, TimeUnit.SECONDS, () -> {
        // Verify that DictionaryRetrievalService#handleVersionRetirement is never called for the existing version
        verify(compressorFactory, never()).removeVersionSpecificCompressor(KAFKA_TOPIC_NAME);
        // Verify that the downloading future map contains this version
        assertTrue(downloadingDictionaryFutures.containsKey(KAFKA_TOPIC_NAME));
      });

      // Now make version 1 retired
      doReturn(null).when(store).getVersion(VERSION_NUMBER);
      storeChangeListener.handleStoreChanged(store);
      TestUtils.waitForNonDeterministicAssertion(MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS, TimeUnit.SECONDS, () -> {
        // Verify that DictionaryRetrievalService#handleVersionRetirement is called for the retired version
        verify(compressorFactory, atLeastOnce()).removeVersionSpecificCompressor(KAFKA_TOPIC_NAME);
      });
    } finally {
      if (dictionaryRetrievalService != null) {
        dictionaryRetrievalService.stop();
        dictionaryRetrievalService.close();
        // reset to NO_OP for next test
        doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
        // Reset version 1 back to active
        doReturn(version).when(store).getVersion(VERSION_NUMBER);
      }
    }
  }
}
