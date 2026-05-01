package com.linkedin.venice.client.store.transport;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.d2.balancer.D2Client;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link D2TransportClient}'s push hook — the listener fires exactly when the
 * underlying D2 service name actually changes, so {@code BasicClientStats} can swap its
 * {@code venice.cluster.name} dimension on initial discovery and on 301-redirect-driven
 * migrations without per-request polling.
 */
public class D2TransportClientTest {
  /**
   * Wiring a callback and then mutating the service name via {@link D2TransportClient#setServiceName}
   * must invoke the callback exactly once with the new value. This is the bootstrap path that
   * fires when {@code AbstractAvroStoreClient.discoverD2Service} resolves the storage cluster.
   */
  @Test
  public void testSetServiceNameFiresCallbackOnChange() {
    D2TransportClient transport = new D2TransportClient("bootstrap-discovery", mock(D2Client.class));
    AtomicReference<String> received = new AtomicReference<>();

    transport.setServiceNameChangeCallback(received::set);
    transport.setServiceName("venice-cluster-resolved");

    assertEquals(received.get(), "venice-cluster-resolved", "callback should receive the new service name");
  }

  /**
   * Without a callback wired (the common case for tests that construct the transport directly,
   * or for non-stat-tracking client variants), {@code setServiceName} must not throw a
   * {@code NullPointerException}.
   */
  @Test
  public void testSetServiceNameWithNoCallbackDoesNotThrow() {
    D2TransportClient transport = new D2TransportClient("bootstrap", mock(D2Client.class));

    // No setServiceNameChangeCallback wired — this should be safe.
    transport.setServiceName("venice-cluster-X");

    assertEquals(transport.getServiceName(), "venice-cluster-X");
  }

  /**
   * The transport always fires on every {@code setServiceName} — even when the value matches the
   * field — because consumers may have a stale cached value (e.g., {@code BasicClientStats}'s
   * bootstrap {@code "unknown"} sentinel) that doesn't reflect the transport's current state.
   * Dedup is delegated to {@code BasicClientStats.onClusterNameUpdated} which short-circuits on
   * its own {@code currentClusterName}.
   */
  @Test
  public void testSetServiceNameAlwaysFiresCallbackEvenOnSameValue() {
    D2TransportClient transport = new D2TransportClient("venice-cluster-A", mock(D2Client.class));
    StringBuilder observed = new StringBuilder();
    Consumer<String> recorder = name -> observed.append(name).append(';');

    transport.setServiceNameChangeCallback(recorder);
    transport.setServiceName("venice-cluster-A"); // same value as constructor — fires anyway
    transport.setServiceName("venice-cluster-A"); // and again
    transport.setServiceName("venice-cluster-B"); // migration — fires

    assertEquals(observed.toString(), "venice-cluster-A;venice-cluster-A;venice-cluster-B;");
  }
}
