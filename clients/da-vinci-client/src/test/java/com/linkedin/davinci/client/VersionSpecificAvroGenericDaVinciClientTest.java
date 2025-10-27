package com.linkedin.davinci.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VersionSpecificAvroGenericDaVinciClientTest {
  private final VersionSpecificAvroGenericDaVinciClient versionSpecificAvroGenericDaVinciClient =
      mock(VersionSpecificAvroGenericDaVinciClient.class);
  private final DaVinciBackend daVinciBackend = mock(DaVinciBackend.class);
  private final StoreBackend storeBackend = mock(StoreBackend.class);
  private final SubscriptionBasedReadOnlyStoreRepository storeRepository =
      mock(SubscriptionBasedReadOnlyStoreRepository.class);
  private final Store store = mock(Store.class);
  private final Version version = mock(Version.class);
  private final ComplementSet<Integer> partitionsSet = ComplementSet.newSet(Arrays.asList(1));
  private MockedStatic<AvroGenericDaVinciClient> avroGenericDaVinciClient;

  @BeforeMethod
  public void setUp() {
    avroGenericDaVinciClient = mockStatic(AvroGenericDaVinciClient.class);
    avroGenericDaVinciClient.when(AvroGenericDaVinciClient::getBackend).thenReturn(daVinciBackend);
    doCallRealMethod().when(versionSpecificAvroGenericDaVinciClient).subscribe(any(ComplementSet.class));

    when(versionSpecificAvroGenericDaVinciClient.getStoreBackend()).thenReturn(storeBackend);
    Integer storeVersion = 1;
    when(versionSpecificAvroGenericDaVinciClient.getStoreVersion()).thenReturn(storeVersion);
    when(daVinciBackend.getStoreRepository()).thenReturn(storeRepository);

    when(store.getVersion(storeVersion)).thenReturn(version);
    when(storeRepository.getStoreOrThrow((any()))).thenReturn(store);
  }

  @AfterMethod
  public void cleanUp() {
    avroGenericDaVinciClient.close();
  }

  @Test
  public void testSubscribeWithExistingVersion() {
    versionSpecificAvroGenericDaVinciClient.subscribe(partitionsSet);

    verify(versionSpecificAvroGenericDaVinciClient).addPartitionsToSubscription(partitionsSet);
    verify(storeBackend)
        .subscribe(partitionsSet, Optional.of(version), Collections.emptyMap(), null, Collections.emptyMap());
  }

  @Test
  public void testSubscribeWithNonExistingVersion() {
    when(store.getVersion(anyInt())).thenReturn(null);

    assertThrows(VeniceClientException.class, () -> versionSpecificAvroGenericDaVinciClient.subscribe(partitionsSet));
  }
}
