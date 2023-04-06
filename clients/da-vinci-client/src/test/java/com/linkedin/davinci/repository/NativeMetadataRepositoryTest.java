package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryTest {
  private ClientConfig clientConfig;
  private VeniceProperties backendConfig;

  private static final String STORE_NAME = "hardware_store";

  @BeforeClass
  public void setUpMocks() {
    clientConfig = mock(ClientConfig.class);
    backendConfig = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfig).getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
  }

  @Test
  public void testGetInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfig, backendConfig);
    Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(() -> nativeMetadataRepository.start());
  }
}
