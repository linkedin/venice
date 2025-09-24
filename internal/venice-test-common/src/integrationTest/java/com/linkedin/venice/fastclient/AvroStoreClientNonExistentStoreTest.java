package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This is a test class to verify the behavior of AvroStoreClient when attempting to connect to a non-existent store.
 */
public class AvroStoreClientNonExistentStoreTest extends AbstractClientEndToEndSetup {
  private static final String NON_EXISTENT_STORE_NAME = "non_existent_store";
  private static final VeniceMetricsRepository METRICS_REPOSITORY = new VeniceMetricsRepository();

  @Test
  public void testGetAndStartAvroGenericStoreClientWithNonExistentStore() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(NON_EXISTENT_STORE_NAME).setR2Client(r2Client);
    ServiceDiscoveryException exception = Assert.expectThrows(
        ServiceDiscoveryException.class,
        () -> getGenericFastClient(
            clientConfigBuilder,
            METRICS_REPOSITORY,
            StoreMetadataFetchMode.SERVER_BASED_METADATA));
    Assert.assertTrue(exception.getCause() instanceof VeniceNoStoreException);
  }

  @Test
  public void testGetAndStartAvroSpecificStoreClientWithNonExistentStore() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(NON_EXISTENT_STORE_NAME).setR2Client(r2Client);
    ServiceDiscoveryException exception = Assert.expectThrows(
        ServiceDiscoveryException.class,
        () -> getSpecificFastClient(
            clientConfigBuilder,
            METRICS_REPOSITORY,
            SpecificRecord.class,
            StoreMetadataFetchMode.SERVER_BASED_METADATA));
    Assert.assertTrue(exception.getCause() instanceof VeniceNoStoreException);

  }
}
