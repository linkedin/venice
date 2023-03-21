package com.linkedin.venice.fastclient;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class FastClientDaVinciClientCompatTest extends AbstractClientEndToEndSetup {
  CachingDaVinciClientFactory daVinciClientFactory;

  @Test(timeOut = TIME_OUT)
  public void testFastClientDaVinciClientCompatOnRestart() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(false)
            .setSpeculativeQueryEnabled(false)
            .setLongTailRetryEnabledForSingleGet(true)
            .setLongTailRetryThresholdForSingleGetInMicroSeconds(10);

    AvroSpecificStoreClient<String, TestValueSchema> fastClient =
        getSpecificFastClient(clientConfigBuilder, new MetricsRepository(), TestValueSchema.class, false);
    Assert.assertNotNull(fastClient.get("key_1").get());
    try (DaVinciClient<String, TestValueSchema> daVinciClient = setupDaVinciClient(storeName)) {
      daVinciClient.subscribeAll().get();
      Assert.assertNotNull(daVinciClient.get("key_1").get());
    }
    fastClient.close();
    try (DaVinciClient<String, TestValueSchema> daVinciClient = setupDaVinciClient(storeName)) {
      daVinciClient.subscribeAll().get();
      Assert.assertNotNull(daVinciClient.get("key_1").get());
    }
  }

  @Test(timeOut = TIME_OUT)
  public void testFastClientDaVinciClientCompatOnClose() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(false)
            .setSpeculativeQueryEnabled(false)
            .setLongTailRetryEnabledForSingleGet(true)
            .setLongTailRetryThresholdForSingleGetInMicroSeconds(10);
    try (DaVinciClient<String, TestValueSchema> daVinciClient = setupDaVinciClient(storeName)) {
      daVinciClient.subscribeAll().get();
      Assert.assertNotNull(daVinciClient.get("key_1").get());
      AvroSpecificStoreClient<String, TestValueSchema> fastClient =
          getSpecificFastClient(clientConfigBuilder, new MetricsRepository(), TestValueSchema.class, false);
      Assert.assertNotNull(fastClient.get("key_1").get());
      AvroSpecificStoreClient<String, TestValueSchema> fastClient2 =
          getSpecificFastClient(clientConfigBuilder, new MetricsRepository(), TestValueSchema.class, false);
      Assert.assertNotNull(fastClient2.get("key_1").get());
    }
  }

  @AfterMethod
  public void releaseAllDVC() {
    cleanupDaVinciClient();
  }

  private DaVinciClient<String, TestValueSchema> setupDaVinciClient(String storeName) {
    VeniceProperties userStoreDaVinciBackendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY, false)
            .build();
    daVinciClientFactory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        userStoreDaVinciBackendConfig);
    return daVinciClientFactory.getAndStartSpecificAvroClient(storeName, new DaVinciConfig(), TestValueSchema.class);
  }

  private void cleanupDaVinciClient() {
    Utils.closeQuietlyWithErrorLogged(daVinciClientFactory);
  }
}
