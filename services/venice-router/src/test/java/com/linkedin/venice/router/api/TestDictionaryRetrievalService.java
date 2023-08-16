package com.linkedin.venice.router.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDictionaryRetrievalService {
  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testSuppressingRedundantLogs() throws Exception {
    OnlineInstanceFinder onlineInstanceFinder = mock(OnlineInstanceFinder.class);
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    List<Store> stores = new ArrayList();
    Store store = mock(Store.class);
    List<Version> versions = new ArrayList();
    Version version = mock(Version.class);
    doReturn(CompressionStrategy.NO_OP).when(version).getCompressionStrategy();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn("test_store_v1").when(version).kafkaTopicName();
    versions.add(version);
    doReturn(versions).when(store).getVersions();
    doReturn(Optional.ofNullable(version)).when(store).getVersion(anyInt());
    stores.add(store);
    doReturn(stores).when(metadataRepository).getAllStores();
    doReturn(store).when(metadataRepository).getStore(any());
    StorageNodeClient storageNodeClient = mock(StorageNodeClient.class);
    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    SSLFactory sslFactory = mock(SSLFactory.class);
    DictionaryRetrievalService dictionaryRetrievalService = new DictionaryRetrievalService(
        onlineInstanceFinder,
        routerConfig,
        Optional.of(sslFactory),
        metadataRepository,
        storageNodeClient,
        compressorFactory);
    try {
      dictionaryRetrievalService.start();
      StoreDataChangedListener storeChangeListener = dictionaryRetrievalService.getStoreChangeListener();
      // update the compression strategy such that dictionaryRetrievalService will try to fetch the dictionary
      doReturn(CompressionStrategy.ZSTD_WITH_DICT).when(version).getCompressionStrategy();
      storeChangeListener.handleStoreChanged(store);
      RedundantExceptionFilter redundantExceptionFilter = dictionaryRetrievalService.getRedundantExceptionFilter();

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        Assert.assertTrue(
            redundantExceptionFilter.isRedundantException("Beginning dictionary fetch for test_store_v1", false));
        Assert.assertTrue(
            redundantExceptionFilter
                .isRedundantException("Dictionary fetch failed. Store topics were: test_store_v1. null", false));
        Assert.assertTrue(
            redundantExceptionFilter.isRedundantException(
                "Exception encountered when asynchronously downloading dictionary for resource: test_store_v1. com.linkedin.venice.exceptions.VeniceException: No online storage instance for resource: null_v0",
                false));
      });
    } finally {
      dictionaryRetrievalService.stop();
    }
  }
}
