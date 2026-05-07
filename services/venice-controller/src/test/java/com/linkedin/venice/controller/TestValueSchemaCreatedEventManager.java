package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ValueSchemaCreatedListener;
import com.linkedin.venice.schema.SchemaEntry;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class TestValueSchemaCreatedEventManager {
  private static final String STORE_NAME = "testStore";

  @Test
  public void testListenersInvokedInRegistrationOrder() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener listener1 = mock(ValueSchemaCreatedListener.class);
    ValueSchemaCreatedListener listener2 = mock(ValueSchemaCreatedListener.class);
    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");

    manager.addListener(listener1);
    manager.addListener(listener2);

    manager.notifyValueSchemaCreated(STORE_NAME, mockStore(STORE_NAME), schemaEntry, true);

    InOrder inOrder = inOrder(listener1, listener2);
    inOrder.verify(listener1).handleValueSchemaCreated(any(Store.class), eq(schemaEntry), eq(true));
    inOrder.verify(listener2).handleValueSchemaCreated(any(Store.class), eq(schemaEntry), eq(true));
  }

  @Test
  public void testListenerExceptionDoesNotPreventOtherListeners() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener throwing = mock(ValueSchemaCreatedListener.class);
    doThrow(new RuntimeException("boom")).when(throwing).handleValueSchemaCreated(any(), any(), anyBoolean());
    ValueSchemaCreatedListener second = mock(ValueSchemaCreatedListener.class);

    manager.addListener(throwing);
    manager.addListener(second);

    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");
    manager.notifyValueSchemaCreated(STORE_NAME, mockStore(STORE_NAME), schemaEntry, false);

    // Both listeners get the call; the throw is caught and logged.
    verify(throwing, times(1)).handleValueSchemaCreated(any(Store.class), eq(schemaEntry), eq(false));
    verify(second, times(1)).handleValueSchemaCreated(any(Store.class), eq(schemaEntry), eq(false));
  }

  @Test
  public void testNullStoreSkipsListeners() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    manager.addListener(listener);

    manager.notifyValueSchemaCreated(STORE_NAME, null, new SchemaEntry(1, "\"string\""), true);

    verify(listener, never()).handleValueSchemaCreated(any(), any(), anyBoolean());
  }

  @Test
  public void testListenerReceivesReadOnlyStoreSnapshot() {
    ValueSchemaCreatedEventManager manager = new ValueSchemaCreatedEventManager();
    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    manager.addListener(listener);

    Store mutableStore = mockStore(STORE_NAME);
    manager.notifyValueSchemaCreated(STORE_NAME, mutableStore, new SchemaEntry(1, "\"string\""), true);

    ArgumentCaptor<Store> storeCaptor = ArgumentCaptor.forClass(Store.class);
    verify(listener).handleValueSchemaCreated(storeCaptor.capture(), any(SchemaEntry.class), eq(true));
    assertTrue(
        storeCaptor.getValue() instanceof ReadOnlyStore,
        "Listener should receive a ReadOnlyStore wrapper, not the mutable store directly");
  }

  private static Store mockStore(String storeName) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    return store;
  }
}
