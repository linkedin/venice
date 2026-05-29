package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.listeners.StoreConfigSnapshot;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitorConfig;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * End-to-end checks that the Fast Client metadata listeners fire on real cluster events.
 *
 * <p>Listeners must be registered before {@link AvroGenericStoreClient#start()} to observe the initial transition
 * committed by the first metadata refresh. These tests use {@link #getGenericFastClientWithoutStart} so they can
 * register a listener on the returned (unstarted) client and then call {@code start()} themselves. The refresh
 * interval is dropped to 1 second so subsequent refreshes complete within the per-test timeout.
 */
public class FastClientListenerIntegrationTest extends AbstractClientEndToEndSetup {
  @Test(timeOut = TIME_OUT)
  public void versionSwitchListenerFiresOnNewVersionPush() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMetadataRefreshIntervalInSeconds(1L)
            .setInstanceHealthMonitor(
                new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().setClient(r2Client).build()));

    try (AvroGenericStoreClient<String, GenericRecord> client = getGenericFastClientWithoutStart(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA)) {

      BlockingQueue<int[]> transitions = new LinkedBlockingQueue<>();
      client.registerVersionSwitchListener((prev, next) -> transitions.offer(new int[] { prev, next }));
      client.start();

      // First refresh after start() must deliver the initial transition.
      int[] initialTransition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(initialTransition, "version-switch listener must fire on the first refresh after start()");
      int initialVersion = initialTransition[1];

      veniceCluster.useControllerClient(controllerClient -> {
        VersionCreationResponse response =
            controllerClient.emptyPush(storeName, "test-listener-push-" + Utils.getUniqueString(), 1024 * 1024L);
        assertFalse(response.isError(), "emptyPush failed: " + response.getError());
      });

      int[] pushTransition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(pushTransition, "version-switch listener must fire after a new version push");
      assertEquals(pushTransition[0], initialVersion, "previousVersion");
      assertEquals(pushTransition[1], initialVersion + 1, "newVersion");
    }
  }

  @Test(timeOut = TIME_OUT)
  public void storeConfigChangeListenerFiresOnExternalStorageReadModeFlip() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMetadataRefreshIntervalInSeconds(1L)
            .setInstanceHealthMonitor(
                new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().setClient(r2Client).build()));

    try (AvroGenericStoreClient<String, GenericRecord> client = getGenericFastClientWithoutStart(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA)) {

      BlockingQueue<StoreConfigSnapshot[]> transitions = new LinkedBlockingQueue<>();
      client.registerStoreConfigChangeListener(
          (prev, curr) -> transitions.offer(new StoreConfigSnapshot[] { prev, curr }));
      client.start();

      // First transition after start() is (null -> VENICE_ONLY); consume it before triggering the operator flip.
      StoreConfigSnapshot[] initialTransition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(initialTransition, "store-config-change listener must fire on the first refresh after start()");
      assertEquals(
          initialTransition[1].getExternalStorageReadMode(),
          ExternalStorageReadMode.VENICE_ONLY,
          "initial mode");

      veniceCluster.useControllerClient(controllerClient -> {
        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setExternalStorageReadMode(ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN));
        assertFalse(response.isError(), "updateStore failed: " + response.getError());
      });

      StoreConfigSnapshot[] flipTransition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(flipTransition, "store-config-change listener must fire after externalStorageReadMode flip");
      assertEquals(
          flipTransition[0].getExternalStorageReadMode(),
          ExternalStorageReadMode.VENICE_ONLY,
          "previous mode");
      assertEquals(
          flipTransition[1].getExternalStorageReadMode(),
          ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN,
          "current mode");
    }
  }
}
