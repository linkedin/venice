package com.linkedin.venice.client.store.transport;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link D2TransportClient}'s redirect notifier — the hook the upper layer wires
 * to react to 301-redirect-driven store migrations. The notifier fires from the redirect handlers
 * inside {@code restRequest}/{@code streamRequest} after the {@code Location} header authority
 * has been applied. Initial discovery and other paths that update the service name
 * push cluster names through their own channels and do not flow through this hook.
 */
public class D2TransportClientTest {
  /**
   * The redirect notifier's contract is "fired on 301 migration." Programmatic service-name
   * updates via {@link D2TransportClient#setServiceName} are not migrations and must leave the
   * notifier untouched.
   */
  @Test
  public void testSetServiceNameDoesNotFireRedirectNotifier() {
    D2TransportClient transport = new D2TransportClient("bootstrap-discovery", mock(D2Client.class));
    AtomicInteger fireCount = new AtomicInteger();

    transport.setRedirectNotifier(fireCount::incrementAndGet);
    transport.setServiceName("venice-cluster-resolved");
    transport.setServiceName("venice-cluster-resolved-again");

    assertEquals(transport.getServiceName(), "venice-cluster-resolved-again");
    assertEquals(fireCount.get(), 0, "redirect notifier should only fire on 301-driven migrations");
  }

  /**
   * The notifier is optional. Test variants and non-stat-tracking client paths construct the
   * transport without wiring it; {@code setServiceName} must remain safe in that configuration.
   */
  @Test
  public void testSetServiceNameWithNoNotifierDoesNotThrow() {
    D2TransportClient transport = new D2TransportClient("bootstrap", mock(D2Client.class));

    transport.setServiceName("venice-cluster-X");

    assertEquals(transport.getServiceName(), "venice-cluster-X");
  }

  /**
   * Each transport supports exactly one redirect notifier; the upper layer's contract assumes
   * exclusive ownership of the migration-resolution path. Registering a second non-null notifier
   * is a programming error and is rejected at wiring time.
   */
  @Test
  public void testSetRedirectNotifierRejectsSecondRegistration() {
    D2TransportClient transport = new D2TransportClient("bootstrap", mock(D2Client.class));

    transport.setRedirectNotifier(() -> {});

    assertThrows(IllegalStateException.class, () -> transport.setRedirectNotifier(() -> {}));
  }

  /**
   * Passing {@code null} clears the notifier slot. This is required for test teardown and for
   * decorator chains that rebuild their wiring without recreating the underlying transport.
   */
  @Test
  public void testSetRedirectNotifierAllowsRewireAfterUnset() {
    D2TransportClient transport = new D2TransportClient("bootstrap", mock(D2Client.class));

    transport.setRedirectNotifier(() -> {});
    transport.setRedirectNotifier(null);
    // Slot is empty again; another notifier can be registered.
    transport.setRedirectNotifier(() -> {});
  }

  /**
   * The setter ignores null/empty inputs to keep {@code d2ServiceName} from being silently cleared
   * by a misbehaving caller (e.g. a discovery response with a null {@code D2Service}). The current
   * value must survive, and the redirect notifier must not fire — only true 301-driven changes
   * notify upstream.
   */
  @Test
  public void testSetServiceNameIgnoresNullAndEmpty() {
    D2TransportClient transport = new D2TransportClient("bootstrap-discovery", mock(D2Client.class));
    AtomicInteger fireCount = new AtomicInteger();
    transport.setRedirectNotifier(fireCount::incrementAndGet);

    String original = transport.getServiceName();
    transport.setServiceName(null);
    assertEquals(transport.getServiceName(), original, "null must not overwrite the current service name");

    transport.setServiceName("");
    assertEquals(transport.getServiceName(), original, "empty must not overwrite the current service name");

    assertEquals(fireCount.get(), 0, "redirect notifier must not fire on no-op service-name updates");
  }
}
