package com.linkedin.venice.router.api;

import static com.linkedin.venice.router.api.DictionaryRetrievalService.MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS;
import static com.linkedin.venice.router.api.DictionaryRetrievalService.MIN_DICTIONARY_DOWNLOAD_DELAY_TIME_MS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDictionaryRetrievalService {
  private static final Logger LOGGER = LogManager.getLogger(TestDictionaryRetrievalService.class);
  OnlineInstanceFinder onlineInstanceFinder;
  VeniceRouterConfig routerConfig;
  ReadOnlyStoreRepository metadataRepository;
  SSLFactory sslFactory;
  StorageNodeClient storageNodeClient;
  CompressorFactory compressorFactory;
  Store store;
  Version version;

  @BeforeClass
  public void setUp() {
    onlineInstanceFinder = mock(OnlineInstanceFinder.class);
    routerConfig = mock(VeniceRouterConfig.class);
    metadataRepository = mock(ReadOnlyStoreRepository.class);
    List<Store> stores = new ArrayList();
    store = mock(Store.class);
    List<Version> versions = new ArrayList();
    version = mock(Version.class);
    // start with NO_OP for warmup to succeed
    doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn("test_store_v1").when(version).kafkaTopicName();
    versions.add(version);
    doReturn(versions).when(store).getVersions();
    doReturn(version).when(store).getVersion(anyInt());
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
            redundantExceptionFilter.isRedundantException("Beginning dictionary fetch for test_store_v1", false));
        assertTrue(
            redundantExceptionFilter
                .isRedundantException("Dictionary fetch failed. Store topics were: test_store_v1. null", false));
        assertTrue(
            redundantExceptionFilter.isRedundantException(
                "Exception encountered when asynchronously downloading dictionary for resource: test_store_v1. com.linkedin.venice.exceptions.VeniceException: No online storage instance for resource: null_v0",
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
        assertNotNull(finalDictionaryRetrievalService.getFetchDelayTimeinMsMap().get("test_store_v1"));
      });

      // start at a higher retry time as it will be easy to miss the first couple of retries
      long expectedValue = MIN_DICTIONARY_DOWNLOAD_DELAY_TIME_MS * 4;
      while (true) {
        long finalExpectedValue = expectedValue;
        TestUtils.waitForNonDeterministicAssertion(MAX_DICTIONARY_DOWNLOAD_DELAY_TIME_MS, TimeUnit.SECONDS, () -> {
          assertEquals(
              (long) finalDictionaryRetrievalService.getFetchDelayTimeinMsMap().get("test_store_v1"),
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
}
