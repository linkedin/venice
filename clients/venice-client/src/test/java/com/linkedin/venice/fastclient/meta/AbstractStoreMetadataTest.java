package com.linkedin.venice.fastclient.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.RequestContext;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AbstractStoreMetadataTest {
  private static final String STORE_NAME = "testStore";

  @Test
  public void registerAndFireDeliversTransitionToListener() {
    TestStoreMetadata metadata = newMetadata();
    List<int[]> received = new ArrayList<>();
    metadata.registerVersionSwitchListener((prev, next) -> received.add(new int[] { prev, next }));

    metadata.fireVersionSwitch(-1, 3);

    assertEquals(received.size(), 1);
    assertEquals(received.get(0)[0], -1);
    assertEquals(received.get(0)[1], 3);
  }

  @Test
  public void firingWithNoListenersIsSafe() {
    TestStoreMetadata metadata = newMetadata();
    metadata.fireVersionSwitch(-1, 3);
    metadata.fireVersionSwitch(3, 4);
  }

  @Test
  public void multipleListenersAllFire() {
    TestStoreMetadata metadata = newMetadata();
    AtomicInteger a = new AtomicInteger();
    AtomicInteger b = new AtomicInteger();
    metadata.registerVersionSwitchListener((p, n) -> a.incrementAndGet());
    metadata.registerVersionSwitchListener((p, n) -> b.incrementAndGet());

    metadata.fireVersionSwitch(3, 4);

    assertEquals(a.get(), 1);
    assertEquals(b.get(), 1);
  }

  @Test
  public void listenerExceptionDoesNotBlockOtherListeners() {
    TestStoreMetadata metadata = newMetadata();
    AtomicInteger reached = new AtomicInteger();
    metadata.registerVersionSwitchListener((p, n) -> {
      throw new RuntimeException("boom");
    });
    metadata.registerVersionSwitchListener((p, n) -> reached.incrementAndGet());

    metadata.fireVersionSwitch(3, 4);

    assertEquals(reached.get(), 1, "subsequent listener must still receive the transition");
  }

  @Test
  public void unregisterStopsFurtherDeliveries() {
    TestStoreMetadata metadata = newMetadata();
    AtomicInteger received = new AtomicInteger();
    StoreVersionSwitchListener listener = (p, n) -> received.incrementAndGet();
    metadata.registerVersionSwitchListener(listener);
    metadata.fireVersionSwitch(3, 4);

    metadata.unregisterVersionSwitchListener(listener);
    metadata.fireVersionSwitch(4, 5);

    assertEquals(received.get(), 1);
  }

  @Test
  public void registeringSameListenerTwiceFiresOnce() {
    TestStoreMetadata metadata = newMetadata();
    AtomicInteger received = new AtomicInteger();
    StoreVersionSwitchListener listener = (p, n) -> received.incrementAndGet();
    metadata.registerVersionSwitchListener(listener);
    metadata.registerVersionSwitchListener(listener);

    metadata.fireVersionSwitch(3, 4);

    assertEquals(received.get(), 1);
  }

  @Test
  public void registerRejectsNullListener() {
    TestStoreMetadata metadata = newMetadata();
    assertThrows(IllegalArgumentException.class, () -> metadata.registerVersionSwitchListener(null));
  }

  @Test
  public void unregisterTolerantOfNullAndUnknownListeners() {
    TestStoreMetadata metadata = newMetadata();
    metadata.unregisterVersionSwitchListener(null);
    metadata.unregisterVersionSwitchListener((p, n) -> {});
  }

  private TestStoreMetadata newMetadata() {
    return new TestStoreMetadata(STORE_NAME);
  }

  /**
   * Concrete subclass of {@link AbstractStoreMetadata} used purely to drive the listener-plumbing tests. All
   * methods unrelated to version-switch notification are stubbed to throw, which guarantees that any future
   * accidental dependency on them in a listener test would fail loudly rather than silently.
   */
  private static final class TestStoreMetadata extends AbstractStoreMetadata {
    private final String storeName;

    TestStoreMetadata(String storeName) {
      super(buildClientConfig(storeName));
      this.storeName = storeName;
    }

    private static ClientConfig buildClientConfig(String storeName) {
      ClientConfig clientConfig = org.mockito.Mockito.mock(ClientConfig.class);
      org.mockito.Mockito.doReturn(storeName).when(clientConfig).getStoreName();
      org.mockito.Mockito.doReturn(ClientRoutingStrategyType.LEAST_LOADED)
          .when(clientConfig)
          .getClientRoutingStrategyType();
      return clientConfig;
    }

    @Override
    public String getStoreName() {
      return storeName;
    }

    @Override
    public String getClusterName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCurrentStoreVersion() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPartitionId(int version, ByteBuffer key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPartitionId(int version, byte[] key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getReplicas(int version, int partitionId) {
      return Collections.emptyList();
    }

    @Override
    public Schema getKeySchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Schema getValueSchema(int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getValueSchemaId(Schema schema) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Schema getLatestValueSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Integer getLatestValueSchemaId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Schema getUpdateSchema(int valueSchemaId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public com.linkedin.venice.schema.writecompute.DerivedSchemaEntry getLatestUpdateSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBatchGetLimit() {
      return 0;
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
    }

    @Override
    public ChainedCompletableFuture<Integer, Integer> trackHealthBasedOnRequestToInstance(
        String instance,
        int version,
        int partitionId,
        CompletableFuture<TransportClientResponse> transportFuture) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <K> void routeRequest(RequestContext requestContext, RecordSerializer<K> keySerializer) {
      throw new UnsupportedOperationException();
    }

    /** Expose the protected fire method so tests can drive transitions without a full refresh cycle. */
    @Override
    protected void fireVersionSwitch(int previousVersion, int newVersion) {
      super.fireVersionSwitch(previousVersion, newVersion);
    }
  }
}
