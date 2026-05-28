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
 * <p>The refresh interval is dropped to 1 second so each test observes the next refresh within a few seconds rather
 * than waiting for the 60s default. Listeners are registered through the public
 * {@link AvroGenericStoreClient#registerVersionSwitchListener} / {@link AvroGenericStoreClient#registerStoreConfigChangeListener}
 * surface — Fast Client wrappers forward through to the underlying metadata.
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

    try (AvroGenericStoreClient<String, GenericRecord> client = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA)) {

      BlockingQueue<int[]> transitions = new LinkedBlockingQueue<>();
      client.registerVersionSwitchListener((prev, next) -> transitions.offer(new int[] { prev, next }));

      // We need the initial version after the first refresh has committed — capture from the very first transition,
      // not from a getCurrentStoreVersion() snapshot taken pre-refresh.
      int[] initialTransition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(initialTransition, "version-switch listener must fire on the first refresh");
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

    try (AvroGenericStoreClient<String, GenericRecord> client = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA)) {

      BlockingQueue<StoreConfigSnapshot[]> transitions = new LinkedBlockingQueue<>();
      client.registerStoreConfigChangeListener(
          (prev, curr) -> transitions.offer(new StoreConfigSnapshot[] { prev, curr }));

      veniceCluster.useControllerClient(controllerClient -> {
        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setExternalStorageReadMode(ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN));
        assertFalse(response.isError(), "updateStore failed: " + response.getError());
      });

      // The first transition is the initial-snapshot publication (null -> VENICE_ONLY), the second is the operator
      // flip. Drain until we see a non-VENICE_ONLY current mode.
      StoreConfigSnapshot[] flipTransition = null;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        StoreConfigSnapshot[] t = transitions.poll(1, TimeUnit.SECONDS);
        if (t != null && t[1].getExternalStorageReadMode() != ExternalStorageReadMode.VENICE_ONLY) {
          flipTransition = t;
          break;
        }
      }
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
