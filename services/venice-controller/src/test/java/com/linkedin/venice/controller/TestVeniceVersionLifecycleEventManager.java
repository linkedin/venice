package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class TestVeniceVersionLifecycleEventManager {
  @Test
  public void testListenerOrder() {
    VeniceVersionLifecycleEventManager manager = new VeniceVersionLifecycleEventManager();
    VeniceVersionLifecycleEventListener listener1 = mock(VeniceVersionLifecycleEventListener.class);
    VeniceVersionLifecycleEventListener listener2 = mock(VeniceVersionLifecycleEventListener.class);
    VeniceVersionLifecycleEventListener listener3 = mock(VeniceVersionLifecycleEventListener.class);
    Store store = mock(Store.class);
    Version version = mock(Version.class);

    // Register listeners in a specific order
    manager.addListener(listener1);
    manager.addListener(listener2);
    manager.addListener(listener3);

    // Trigger an event
    manager.notifyVersionCreated(store, version, true);

    // Verify that listeners are called in registration order
    InOrder inOrder = inOrder(listener1, listener2, listener3);
    inOrder.verify(listener1).onVersionCreated(any(Store.class), any(Version.class), eq(true));
    inOrder.verify(listener2).onVersionCreated(any(Store.class), any(Version.class), eq(true));
    inOrder.verify(listener3).onVersionCreated(any(Store.class), any(Version.class), eq(true));
  }

  @Test
  public void testAllEventsTriggeredForListener() {
    VeniceVersionLifecycleEventManager manager = new VeniceVersionLifecycleEventManager();
    VeniceVersionLifecycleEventListener listener = mock(VeniceVersionLifecycleEventListener.class);
    Store store = mock(Store.class);
    Version version = mock(Version.class);

    manager.addListener(listener);

    // Trigger all events with both true and false for isSourceCluster
    manager.notifyVersionCreated(store, version, true);
    manager.notifyVersionDeleted(store, version, false);
    manager.notifyVersionBecomingCurrentFromFuture(store, version, true);
    manager.notifyVersionBecomingCurrentFromBackup(store, version, false);
    manager.notifyVersionBecomingBackup(store, version, true);

    // Verify each event method is called with the correct parameters
    verify(listener, times(1)).onVersionCreated(any(Store.class), any(Version.class), eq(true));
    verify(listener, times(1)).onVersionDeleted(any(Store.class), any(Version.class), eq(false));
    verify(listener, times(1)).onVersionBecomingCurrentFromFuture(any(Store.class), any(Version.class), eq(true));
    verify(listener, times(1)).onVersionBecomingCurrentFromBackup(any(Store.class), any(Version.class), eq(false));
    verify(listener, times(1)).onVersionBecomingBackup(any(Store.class), any(Version.class), eq(true));
  }
}
