package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitorConfig;
import com.linkedin.venice.fastclient.meta.StoreConfigSnapshot;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * End-to-end checks that the Fast Client metadata listeners fire on real cluster events.
 *
 * <p>The refresh interval is dropped to 1 second so each test observes the next refresh within a few seconds rather
 * than waiting for the 60s default. The {@code StoreMetadata} is reached by walking the Fast Client wrapper chain
 * via reflection (the {@code getStoreMetadata()} accessor on {@code DispatchingAvroGenericStoreClient} is
 * package-protected and only reachable on the dispatching layer itself).
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

      StoreMetadata metadata = extractStoreMetadata(client);
      int initialVersion = metadata.getCurrentStoreVersion();

      BlockingQueue<int[]> transitions = new LinkedBlockingQueue<>();
      metadata.registerVersionSwitchListener((prev, next) -> transitions.offer(new int[] { prev, next }));

      veniceCluster.useControllerClient(controllerClient -> {
        VersionCreationResponse response =
            controllerClient.emptyPush(storeName, "test-listener-push-" + Utils.getUniqueString(), 1024 * 1024L);
        assertFalse(response.isError(), "emptyPush failed: " + response.getError());
      });

      int[] transition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(transition, "version-switch listener must fire after a new version push");
      assertEquals(transition[0], initialVersion, "previousVersion");
      assertEquals(transition[1], initialVersion + 1, "newVersion");
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

      StoreMetadata metadata = extractStoreMetadata(client);

      BlockingQueue<StoreConfigSnapshot[]> transitions = new LinkedBlockingQueue<>();
      metadata.registerStoreConfigChangeListener(
          (prev, curr) -> transitions.offer(new StoreConfigSnapshot[] { prev, curr }));

      veniceCluster.useControllerClient(controllerClient -> {
        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setExternalStorageReadMode(ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN));
        assertFalse(response.isError(), "updateStore failed: " + response.getError());
      });

      StoreConfigSnapshot[] transition = transitions.poll(30, TimeUnit.SECONDS);
      assertNotNull(transition, "store-config-change listener must fire after externalStorageReadMode flip");
      assertNotNull(transition[0], "previous snapshot must be non-null after registration on a started client");
      assertEquals(transition[0].getExternalStorageReadMode(), ExternalStorageReadMode.VENICE_ONLY, "previous mode");
      assertEquals(
          transition[1].getExternalStorageReadMode(),
          ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN,
          "current mode");
    }
  }

  /**
   * Walks the Fast Client wrapper chain from the top-level client returned by {@code ClientFactory} down to the
   * {@link DispatchingAvroGenericStoreClient} layer, then returns the {@link StoreMetadata} held there. Reflection
   * is used to traverse the chain because the {@code delegate} / {@code innerStoreClient} fields are
   * package-private. The final {@code getStoreMetadata()} call is reachable directly because this test lives in the
   * same package as {@code DispatchingAvroGenericStoreClient}.
   */
  private static StoreMetadata extractStoreMetadata(AvroGenericStoreClient<?, ?> client) throws Exception {
    Object current = client;
    while (current != null) {
      if (current instanceof DispatchingAvroGenericStoreClient) {
        return ((DispatchingAvroGenericStoreClient<?, ?>) current).getStoreMetadata();
      }
      Object next = null;
      for (Class<?> c = current.getClass(); c != null && next == null; c = c.getSuperclass()) {
        for (String fieldName: new String[] { "delegate", "innerStoreClient" }) {
          try {
            Field f = c.getDeclaredField(fieldName);
            f.setAccessible(true);
            next = f.get(current);
            break;
          } catch (NoSuchFieldException ignored) {
            // Try the next candidate.
          }
        }
      }
      if (next == null) {
        throw new IllegalStateException(
            "Could not find delegate / innerStoreClient field on " + current.getClass().getName());
      }
      current = next;
    }
    throw new IllegalStateException("DispatchingAvroGenericStoreClient not found in wrapper chain");
  }
}
